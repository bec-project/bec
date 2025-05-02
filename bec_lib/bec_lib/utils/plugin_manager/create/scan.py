import typer

_app = typer.Typer()


@_app.command()
def scan(name: str):
    """Create a new scan plugin with the given name"""
    raise NotImplementedError()
