import os
import sys
import pandas as pd
from pandas import DataFrame, read_csv, Index, Timedelta, NaT
import pathlib
import pytz
from numpy import empty
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendar_utils import register_calendar_alias

from exchange_calendars.calendar_utils import ExchangeCalendarDispatcher
from exchange_calendars.errors import InvalidCalendarName

from zipline.data.bundles import core as bundles

from zipline.data.bundles import register

from logbook import Logger, StreamHandler

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)


def _fabricate(self, name: str, **kwargs):
    """Fabricate calendar with `name` and `**kwargs`."""
    try:
        factory = self._calendar_factories[name]
    except KeyError as e:
        raise InvalidCalendarName(calendar_name=name) from e
    if name in ["us_futures", "CMES", "XNYS"]:
        # exchange_calendars has a different default start data
        # that we need to overwrite in order to pass the legacy tests
        setattr(factory, "default_start", pd.Timestamp("1950-01-01", tz=pytz.UTC))
        # kwargs["start"] = pd.Timestamp("1990-01-01", tz="UTC")
    if name not in ["us_futures", "24/7", "24/5", "CMES"]:
        # Zipline had default open time of t+1min
        # factory.open_times = [
        #     (d, t.replace(minute=t.minute + 1)) for d, t in factory.open_times
        # ]
        pass
    calendar = factory(**kwargs)
    self._factory_output_cache[name] = (calendar, kwargs)
    return calendar


# Yay! Monkey patching
ExchangeCalendarDispatcher._fabricate = _fabricate


def csvdir_equities(tframes=None, csvdir=None):
    """
    Generate an ingest function for custom data bundle
    This function can be used in ~/.zipline/extension.py
    to register bundle with custom parameters, e.g. with
    a custom trading calendar.

    Parameters
    ----------
    tframes: tuple, optional
        The data time frames, supported timeframes: 'daily' and 'minute'
    csvdir : string, optional, default: CSVDIR environment variable
        The path to the directory of this structure:
        <directory>/<timeframe1>/<symbol1>.csv
        <directory>/<timeframe1>/<symbol2>.csv
        <directory>/<timeframe1>/<symbol3>.csv
        <directory>/<timeframe2>/<symbol1>.csv
        <directory>/<timeframe2>/<symbol2>.csv
        <directory>/<timeframe2>/<symbol3>.csv

    Returns
    -------
    ingest : callable
        The bundle ingest function

    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(["daily", "minute"],
                '/full/path/to/the/csvdir/directory'))
    """

    return CSVDIRBundle(tframes, csvdir).ingest


class CSVDIRBundle:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes=None, csvdir=None):
        self.tframes = tframes
        self.csvdir = csvdir

    def ingest(
        self,
        environ,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        output_dir,
    ):

        csvdir_bundle(
            environ,
            asset_db_writer,
            minute_bar_writer,
            daily_bar_writer,
            adjustment_writer,
            calendar,
            start_session,
            end_session,
            cache,
            show_progress,
            output_dir,
            self.tframes,
            self.csvdir,
        )


@bundles.register("csvdir")
def csvdir_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
    tframes=None,
    csvdir=None,
):
    """
    Build a zipline data bundle from the directory with csv files.
    """
    if not csvdir:
        csvdir = environ.get("CSVDIR")
        if not csvdir:
            raise ValueError("CSVDIR environment variable is not set")

    if not os.path.isdir(csvdir):
        raise ValueError("%s is not a directory" % csvdir)

    if not tframes:
        tframes = set(["daily", "minute"]).intersection(os.listdir(csvdir))

        if not tframes:
            raise ValueError(
                "'daily' and 'minute' directories " "not found in '%s'" % csvdir
            )

    divs_splits = {
        "divs": pd.DataFrame(
            columns=[
                "sid",
                "amount",
                "ex_date",
                "record_date",
                "declared_date",
                "pay_date",
            ]
        ),
        "splits": pd.DataFrame(columns=["sid", "ratio", "effective_date"]),
    }
    for i, tframe in enumerate(tframes):
        ddir = os.path.join(csvdir, tframe)

        symbols = sorted(
            item.split(".csv")[0] for item in os.listdir(ddir) if ".csv" in item
        )
        if not symbols:
            raise ValueError("no <symbol>.csv* files found in %s" % ddir)

        dtype = [
            ("start_date", "datetime64[ns]"),
            ("end_date", "datetime64[ns]"),
            ("auto_close_date", "datetime64[ns]"),
            ("symbol", "object"),
        ]
        metadata = pd.DataFrame(empty(len(symbols), dtype=dtype))

        if tframe == "minute":
            writer = minute_bar_writer
        else:
            writer = daily_bar_writer

        writer.write(
            _pricing_iter(ddir, symbols, metadata, divs_splits, show_progress),
            show_progress=show_progress,
        )

        if i == 0:
            # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
            # register "CSVDIR" to resolve to the NYSE calendar, because these
            # are all equities and thus can use the NYSE calendar.
            metadata["exchange"] = "CSVDIR"

            asset_db_writer.write(equities=metadata)

        divs_splits["divs"]["sid"] = divs_splits["divs"]["sid"].astype(int)
        divs_splits["splits"]["sid"] = divs_splits["splits"]["sid"].astype(int)
        adjustment_writer.write(
            splits=divs_splits["splits"], dividends=divs_splits["divs"]
        )


def _pricing_iter(csvdir, symbols, metadata, divs_splits, show_progress):
    with maybe_show_progress(
        symbols, show_progress, label="Loading custom pricing data: "
    ) as it:
        # using scandir instead of listdir can be faster
        files = os.listdir(csvdir)
        file_names = set(f.split('.')[0] for f in files)

        for sid, symbol in enumerate(it):
            logger.debug(f"{symbol}: sid {sid}")

            if symbol not in file_names:
                raise ValueError(f"{symbol}.csv file is not in {csvdir}")

            # NOTE: read_csv can also read compressed csv files
            dfr = pd.read_csv(
                os.path.join(csvdir, f'{symbol}.csv'),
                parse_dates=[0],
                infer_datetime_format=True,
                index_col=0,
            ).sort_index()

            start_date = dfr.index[0]
            end_date = dfr.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)
            metadata.iloc[sid] = start_date, end_date, ac_date, symbol

            if "split" in dfr.columns:
                tmp = 1.0 / dfr[dfr["split"] != 1.0]["split"]
                split = pd.DataFrame(
                    data=tmp.index.tolist(), columns=["effective_date"]
                )
                split["ratio"] = tmp.tolist()
                split["sid"] = sid

                splits = divs_splits["splits"]
                index = pd.Index(
                    range(splits.shape[0], splits.shape[0] + split.shape[0])
                )
                split.set_index(index, inplace=True)
                divs_splits["splits"] = splits.append(split)

            if "dividend" in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = dfr[dfr["dividend"] != 0.0]["dividend"]
                div = pd.DataFrame(data=tmp.index.tolist(), columns=["ex_date"])
                div["record_date"] = pd.NaT
                div["declared_date"] = pd.NaT
                div["pay_date"] = pd.NaT
                div["amount"] = tmp.tolist()
                div["sid"] = sid

                divs = divs_splits["divs"]
                ind = pd.Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                divs_splits["divs"] = divs.append(div)

            yield sid, dfr


register(
    'yahoo-csv',
    csvdir_equities(
        ['minute', 'daily'],
        str(pathlib.Path.home() / '.zipline/csv/yahoo'),
    ),
    calendar_name='NYSE',
)


register(
    'tiingo-csv',
    csvdir_equities(
        ['daily'],
        str(pathlib.Path.home() / '.zipline/csv/tiingo'),
    ),
    calendar_name='NYSE',  # US equities
)


register(
    'tiingo-minute-csv',
    csvdir_equities(
        ['minute', 'daily'],
        str(pathlib.Path.home() / '.zipline/csv/tiingo'),
    ),
    calendar_name='NYSE',  # US equities
    minutes_per_day=391,
)
