import typer

_app = typer.Typer(rich_markup_mode="rich")


@_app.command()
def device(name: str):
    """Create a new device plugin with the given name."""
    raise NotImplementedError()
