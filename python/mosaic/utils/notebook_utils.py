import os


class NotebookUtils:
    @staticmethod
    def displayHTML(html: str):
        if os.environ.get("MOSAIC_JUPYTER", "FALSE") == "TRUE":
            with open("./mosaic_kepler_view.html", "w") as f:
                f.write(html)
        print(html)
