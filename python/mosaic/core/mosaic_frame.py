from typing import Optional

from py4j.java_gateway import JavaClass, JavaObject
from pyspark.sql import DataFrame, SQLContext

from mosaic.config import config


class MosaicFrame(DataFrame):
    _mosaicFrameClass: JavaClass
    _mosaicFrameObject: JavaObject
    _mosaicFrame: JavaObject
    _df: JavaObject
    _geometry_column_name: str

    def __init__(self, df: DataFrame, geometry_column_name: str):
        super(MosaicFrame, self).__init__(df._jdf, config.sql_context)
        self._df = df._jdf
        self._geometry_column_name = geometry_column_name
        self.sc = config.mosaic_spark.sparkContext
        self._mosaicFrameClass = getattr(
            self.sc._jvm.com.databricks.labs.mosaic.sql, "MosaicFrame$"
        )
        self._mosaicFrameObject = getattr(self._mosaicFrameClass, "MODULE$")
        self._mosaicFrame = self._mosaicFrameObject.apply(
            self._df, self._geometry_column_name
        )
        """
        The MosaicFrame class provides convenience functions for indexing and joining spatial dataframes.

        Attributes
        ----------
        df: DataFrame
            A Spark DataFrame
        geometry_column_name: str
            The name of the primary geometry in this spatial dataframe.
        """

    def get_optimal_resolution_str(
            self, sample_rows: Optional[int] = None, sample_fraction: Optional[float] = None
        ) -> str:
        """
        Analyzes the geometries in the currently selected geometry column and proposes an optimal
        grid-index resolution.

        Provide either `sample_rows` or `sample_fraction` parameters to control how much data is passed to the analyzer.
        (Providing too little data to the analyzer may result in a `NotEnoughGeometriesException`)

        Parameters
        ----------
        sample_rows: int, optional
            The number of rows to sample.
        sample_fraction: float, optional
            The proportion of rows to sample.

        Returns
        -------
        str
            The recommended grid-index resolution to apply to this MosaicFrame.
        """
        optionClass = getattr(self.sc._jvm.scala, "Option$")
        optionModule = getattr(optionClass, "MODULE$")
        sampleStrategyClass = getattr(
            self.sc._jvm.com.databricks.labs.mosaic.sql, "SampleStrategy"
        )
        if sample_rows:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(None), optionModule.apply(sample_rows)
            )
            return self._mosaicFrame.analyzer().getOptimalResolutionStr(sampleStrategy)
        if sample_fraction:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(sample_fraction), optionModule.apply(None)
            )
            return self._mosaicFrame.analyzer().getOptimalResolutionStr(sampleStrategy)
        return self._mosaicFrame.analyzer().getOptimalResolutionStr()


    def get_optimal_resolution(
        self, sample_rows: Optional[int] = None, sample_fraction: Optional[float] = None
    ) -> int:
        """
        Analyzes the geometries in the currently selected geometry column and proposes an optimal
        grid-index resolution.

        Provide either `sample_rows` or `sample_fraction` parameters to control how much data is passed to the analyzer.
        (Providing too little data to the analyzer may result in a `NotEnoughGeometriesException`)

        Parameters
        ----------
        sample_rows: int, optional
            The number of rows to sample.
        sample_fraction: float, optional
            The proportion of rows to sample.

        Returns
        -------
        int
            The recommended grid-index resolution to apply to this MosaicFrame.
        """
        optionClass = getattr(self.sc._jvm.scala, "Option$")
        optionModule = getattr(optionClass, "MODULE$")
        sampleStrategyClass = getattr(
            self.sc._jvm.com.databricks.labs.mosaic.sql, "SampleStrategy"
        )
        if sample_rows:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(None), optionModule.apply(sample_rows)
            )
            return self._mosaicFrame.analyzer().getOptimalResolution(sampleStrategy)
        if sample_fraction:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(sample_fraction), optionModule.apply(None)
            )
            return self._mosaicFrame.analyzer().getOptimalResolution(sampleStrategy)
        return self._mosaicFrame.analyzer().getOptimalResolution()

    def get_resolution_metrics(
        self, sample_rows: Optional[int] = None, sample_fraction: Optional[float] = None
    ) -> "DataFrame":
        """
        Analyzes the geometries in the currently selected geometry column and provide statistics
        about grid-index resolutions to help the end user select the optimal resolution in an
        informed manner.

        Provide either `sample_rows` or `sample_fraction` parameters to control how much data is passed to the analyzer.
        (Providing too little data to the analyzer may result in a `NotEnoughGeometriesException`)

        Parameters
        ----------
        sample_rows: int, optional
            The number of rows to sample.
        sample_fraction: float, optional
            The proportion of rows to sample.

        Returns
        -------
        DataFrame
            A dataframe containing statistics for different available resolutions.
        """
        optionClass = getattr(self.sc._jvm.scala, "Option$")
        optionModule = getattr(optionClass, "MODULE$")
        sampleStrategyClass = getattr(
            self.sc._jvm.com.databricks.labs.mosaic.sql, "SampleStrategy"
        )
        analyzer = self._mosaicFrame.analyzer()
        defaultSampleStrategy = getattr(analyzer, "getResolutionMetrics$default$1")()
        defaultLowCount = getattr(analyzer, "getResolutionMetrics$default$2")()
        defaultHighCount = getattr(analyzer, "getResolutionMetrics$default$3")()
        if sample_rows:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(None), optionModule.apply(sample_rows)
            )
            df = self._mosaicFrame.analyzer().getResolutionMetrics(
                sampleStrategy, defaultLowCount, defaultHighCount
            )
            return DataFrame(df, SQLContext(self.sc))
        if sample_fraction:
            sampleStrategy = sampleStrategyClass(
                optionModule.apply(sample_fraction), optionModule.apply(None)
            )
            df = self._mosaicFrame.analyzer().getResolutionMetrics(
                sampleStrategy, defaultLowCount, defaultHighCount
            )
            return DataFrame(df, SQLContext(self.sc))
        df = self._mosaicFrame.analyzer().getResolutionMetrics(
            defaultSampleStrategy, defaultLowCount, defaultHighCount
        )
        return DataFrame(df, SQLContext(self.sc))

    def set_index_resolution(self, resolution: int) -> "MosaicFrame":
        """
        Sets the index resolution for this MosaicFrame.

        Parameters
        ----------
        resolution: int
            The index resolution to use.

        Returns
        -------
        MosaicFrame
            A new instance of the MosaicFrame.
        """
        self._mosaicFrame = self._mosaicFrame.setIndexResolution(resolution)
        return self

    def apply_index(self) -> "MosaicFrame":
        """
        Applies the currently selected indexing strategy to this MosaicFrame.

        Returns
        -------
        MosaicFrame
            A new instance of the MosaicFrame.
        """
        self._mosaicFrame = self._mosaicFrame.applyIndex(True, True)
        return self

    def join(self, other: "MosaicFrame") -> "MosaicFrame":
        """
        Joins this MosaicFrame to `other`.

        Both MosaicFrame instances must be indexed before calling this method.

        Parameters
        ----------
        other: MosaicFrame

        Returns
        -------
        MosaicFrame
            The result of joining this MosaicFrame to `other`.
        """
        self._mosaicFrame = self._mosaicFrame.join(other._mosaicFrame)
        return self

    @property
    def geometry_column(self):
        """
        Returns the currently selected geometry in the MosaicFrame.

        Returns
        -------
        str
            The column name of the currently selected geometry.
        """
        return self._mosaicFrame.getFocalGeometryColumnName()

    def set_geometry_column(self, column_name: str) -> "MosaicFrame":
        """
        Updates the currently selected geometry in the MosaicFrame.

        Parameters
        ----------
        column_name: str
            The column name of the geometry to be selected.

        Returns
        -------
        MosaicFrame
            A new instance of the MosaicFrame.
        """
        self._mosaicFrame = self._mosaicFrame.setGeometryColumn(column_name)
        return self

    def _prettified(self) -> DataFrame:
        return self._mosaicFrame.prettified
