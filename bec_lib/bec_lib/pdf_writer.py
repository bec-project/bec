"""
This module contains a class for writing pdfs.
"""

import datetime

from fpdf import FPDF, XPos, YPos


class BECPDF(FPDF):
    """Custom PDF class for BEC."""

    def header(self):
        if not hasattr(self, "title"):
            return
        if self.title is None:
            self.title = ""
        # Arial bold 15
        self.set_font("Courier", "", 8)
        # Calculate width of title and position
        w = self.get_string_width(self.title) + 6
        self.set_x((210 - w) / 2)
        # Title
        self.cell(w, 9, self.title, 0, new_x=XPos.RIGHT, new_y=YPos.TOP, align="C")
        # Line break
        self.ln(10)

    def footer(self):
        # Position at 1.5 cm from bottom
        self.set_y(-15)
        # Arial italic 8
        self.set_font("Courier", "", 8)
        # Text color in gray
        self.set_text_color(128)
        # date
        self.cell(
            0,
            10,
            f"BEC, {datetime.datetime.now()!s}",  # noqa: DTZ005 - Keep the established local-time display and filename format.
            0,
            new_x=XPos.RIGHT,
            new_y=YPos.TOP,
            align="L",
        )
        # Page number
        self.cell(
            0, 10, "Page " + str(self.page_no()), 0, new_x=XPos.RIGHT, new_y=YPos.TOP, align="R"
        )


class PDFWriter:
    """Class for writing pdfs."""

    LEFT_MARGIN = 25
    TOP_MARGIN = 20
    RIGHT_MARGIN = 20
    FONT_SIZE = 10

    def __init__(self, file: str, title: str | None = None, font="Courier") -> None:
        """Filer writer for pdfs.

        Args:
            file (str): full path to the output file (including file extension).
            title (str, optional): Title of the document. Defaults to None.

        """
        self.file = file
        self.title = title
        self.font = font
        self._pdf = BECPDF(orientation="P", unit="mm", format="A4")
        if self.title:
            self._pdf.set_title(self.title)
        self._pdf.add_page()
        self._pdf.set_margins(left=self.LEFT_MARGIN, top=self.TOP_MARGIN, right=self.RIGHT_MARGIN)
        self._pdf.set_font(self.font, "", self.FONT_SIZE)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def write(self, text: str):
        """add text to the pdf"""
        self._pdf.set_x(self.LEFT_MARGIN)
        self._pdf.multi_cell(0, self.FONT_SIZE // 2, text)

    def close(self):
        """close the pdf and write to disk"""
        self._pdf.output(self.file)


if __name__ == "__main__":  # pragma: no cover
    header = (
        " \n" * 3
        + "  :::            :::       :::   :::   ::::    ::: ::::::::::: \n"
        + "  :+:          :+: :+:    :+:+: :+:+:  :+:+:   :+:     :+:     \n"
        + "  +:+         +:+   +:+  +:+ +:+:+ +:+ :+:+:+  +:+     +:+     \n"
        + "  +#+        +#++:++#++: +#+  +:+  +#+ +#+ +:+ +#+     +#+     \n"
        + "  +#+        +#+     +#+ +#+       +#+ +#+  +#+#+#     +#+     \n"
        + "  #+#        #+#     #+# #+#       #+# #+#   #+#+#     #+#     \n"
        + "  ########## ###     ### ###       ### ###    #### ########### \n"
    )
    with PDFWriter("test_output.pdf") as file:
        file.write(header)
