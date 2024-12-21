__all__ = [
  "Utils",
]

class Utils:

    @staticmethod
    def path_as_fuse(path:str) -> str:
        """Convert 'file:' and 'dbfs:' paths to fuse.

        Handles the following rules:
        - 'dbfs:/Volumes/*' -> '/Volumes/*'
        - 'dbfs:/*' -> '/dbfs/*'
        - 'file:/*' -> '/*'

        Parameters
        ----------
        path : str
            The path to standardize as fuse

        Returns
        -------
        str
            Path standardized as fuse; otherwise the path provided
        """
        p = path
        if path.startswith("dbfs:/Volumes/"):
          p = path.replace("dbfs:/Volumes/", "/Volumes/")
        elif path.startswith("dbfs:/"):
          p = path.replace("dbfs:/", "/dbfs/")
        elif path.startswith("file:/"):
          p = path.replace("file:/", "/")
        return p