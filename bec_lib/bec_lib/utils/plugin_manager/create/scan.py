import typer

_app = typer.Typer(rich_markup_mode="rich")


@_app.command()
def scan(name: str):
    """Create a new scan plugin with the given name."""
    raise NotImplementedError()
