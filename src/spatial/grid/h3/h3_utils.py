__all__ = [
    "H3Utils",
]

from typing import Union

import geopandas as gpd
import pandas as pd
import shapely

class H3Utils:

    @staticmethod
    def h3_to_int_pdf(
            pdf:pd.DataFrame, h3_str_col:str, new_h3_int_col:str = "cellid_int", drop_h3_str_col:bool=False
    ) -> pd.DataFrame:
        """Add a h3 int column a pandas dataframe with a str h3 column.

        Parameters
        ----------
        pdf : pd.DataFrame
            pandas dataframe to add the column
        h3_str_col : str
            Name of the existing int h3 cellid column in the pandas dataframe
        new_h3_int_col : str (optional)
            Name of the new h3 int cellid column in the pandas dataframe;
            default is 'cellid_int'
        drop_h3_str_col : bool (optional)
            Whether to drop the existing h3 str column in the pandas dataframe after
            generating the int h3 column; default is False

        Returns
        -------
        pd.DataFrame
            pandas dataframe with the <new_h3_col_str> populated from the <h3_int_col>
        """
        import h3

        pdf[new_h3_int_col] = [h3.string_to_h3(x) for x in pdf[h3_str_col]]
        if drop_h3_str_col:
            pdf.drop(h3_str_col, axis=1, inplace=True)
        return pdf

    @staticmethod
    def h3_to_str_pdf(
            pdf:pd.DataFrame, h3_int_col:str, new_h3_str_col:str = "cellid_str", drop_h3_int_col:bool=False
    ) -> pd.DataFrame:
        """Add a h3 str column a pandas dataframe with an int h3 column.

        Parameters
        ----------
        pdf : pd.DataFrame
            pandas dataframe to add the column
        h3_int_col : str
            Name of the existing int h3 cellid column in the pandas dataframe
        new_h3_str_col : str (optional)
            Name of the new h3 cellid str column in the pandas dataframe;
            default is 'cellid_str'
        drop_h3_int_col : bool (optional)
            Whether to drop the existing h3 int column in the pandas dataframe after
            generating the str h3 column; default is False
        Returns
        -------
        pd.DataFrame
            pandas dataframe with the <new_h3_int_col> populated from the <h3_str_col>
        """
        import h3

        pdf[new_h3_str_col] = [h3.h3_to_string(x) for x in pdf[h3_int_col]]
        if drop_h3_int_col:
            pdf.drop(h3_int_col, axis=1, inplace=True)
        return pdf

    @staticmethod
    def h3_pdf_to_gdf(pdf:pd.DataFrame, h3_col:str) -> gpd.GeoDataFrame:
        """Convert a pandas dataframe with a h3 column to a geopandas dataframe.

        Parameters
        ----------
        pdf : pd.DataFrame
            pandas dataframe to convert
        h3_col : str
            Name of the h3 cellid column in the pandas dataframe;
            datatype in the h3 column itself may be string or int
        Returns
        -------
        gpd.GeoDataFrame
            Geopandas dataframe with the 'geometry' column populated from the <h3_col>
        """
        import geopandas as gpd

        pdf_cp = pdf.copy(deep=True)
        pdf_cp['geometry'] = [H3Utils.try_cellid_to_shapely(x) for x in pdf_cp[h3_col]]
        return gpd.GeoDataFrame(pdf_cp, geometry='geometry')

    @staticmethod
    def get_res_info(res:int) -> dict:
        """Get average h3 area and edge length in meters for the resolution.

        Parameters
        ----------
        res : int
            The h3 resolution to get info
        Returns
        -------
        dict
            Keys 'area_meters' and 'edge_length_meters
        """
        import h3

        return {
            'area_meters': h3.hex_area(res, unit='m^2'),
            'edge_length_meters': h3.edge_length(res, unit='m')
        }

    @staticmethod
    def print_res_info(res:int):
        """Prints average h3 area and edge length in meters for the resolution.

        Parameters
        ----------
        res : int
            The h3 resolution to print info
        """
        d = H3Utils.get_res_info(res)
        print(f"area in meters? {d['area_meters']:,}")
        print(f"edge length in meters? {d['edge_length_meters']:,}")

    @staticmethod
    def try_cellid_to_shapely(cellid:str|int) -> shapely.Polygon:
        """Convert h3 cellid to shapely polygon.

        Handle coordinate order.
        - h3 order is (y,x)
        - shapely order is (x,y)

        Parameters
        ----------
        cellid : str | int
            The cell id to convert to shapely
        Returns
        -------
        Shapely Polygon
            The geo boundary of the cell id if successful; otherwise, None
        """
        import h3
        import shapely

        try:
            # ensure cell is a string
            cell = cellid
            if isinstance(cellid, int):
                cell = h3.h3_to_string(cellid)

            coords = h3.h3_to_geo_boundary(cell)
            flipped = tuple(coord[::-1] for coord in coords)
            return shapely.Polygon(flipped)
        except Exception as e:
            print(f"ERROR `try_cellid_to_shapely`: {e}")
            pass