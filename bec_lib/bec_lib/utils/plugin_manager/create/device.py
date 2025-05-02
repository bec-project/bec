import typer

_app = typer.Typer()


@_app.command()
def device(name: str):
    """Create a new device plugin with the given name"""
    raise NotImplementedError()
