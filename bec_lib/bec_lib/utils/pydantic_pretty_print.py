from pydantic import BaseModel, ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def pretty_print_pydantic_validation_error(
    exc: ValidationError, context: str | None = None, model: type[BaseModel] | None = None
) -> None:
    """
    Pretty print a Pydantic ValidationError using rich formatting.

    Args:
        exc (ValidationError): The Pydantic ValidationError to pretty print.
        context (str | None): Context or description of where the error occurred.
        model (type[BaseModel] | None): The Pydantic model that raised the ValidationError.
    """

    if context is None and hasattr(exc, "context"):
        context = exc.context  # type: ignore
    if model is None and hasattr(exc, "model"):
        model = exc.model  # type: ignore

    console = Console()

    # Create the main table
    table = Table(
        title="[bold red]Validation Errors[/bold red]",
        show_header=True,
        header_style="bold cyan",
        border_style="red",
        title_style="bold red",
    )

    table.add_column("Field", style="yellow", no_wrap=False)
    table.add_column("Error Type", style="magenta")
    table.add_column("Message", style="white")
    table.add_column("Input", style="dim")

    # Process each error
    for error in exc.errors():
        # Format the location (field path)
        field_parts = []
        for loc in error.get("loc", []):
            field_parts.append(str(loc))
        field = " → ".join(field_parts) if field_parts else "[root]"

        # Get error type
        error_type = error.get("type", "unknown")

        # Get the message
        message = error.get("msg", "No message provided")

        # For missing fields, enhance the message with the expected type from the model
        if error_type == "missing" and model is not None:
            # Get the field info from the model
            field_name = error.get("loc", [])[-1] if error.get("loc") else None
            if field_name and hasattr(model, "model_fields"):
                field_info = model.model_fields.get(field_name)
                if field_info:
                    field_type = field_info.annotation
                    # Get a readable type name
                    type_name = getattr(field_type, "__name__", str(field_type))
                    message = f"{message} (expected type: {type_name})"

        # Get the input value (if available)
        # For missing fields, don't show the entire parent object
        if error_type == "missing":
            input_str = "[dim italic]missing[/dim italic]"
        else:
            input_value = error.get("input", "")
            if input_value == "":
                input_str = "[dim italic]N/A[/dim italic]"
            else:
                # Truncate long inputs
                input_str = str(input_value)
                if len(input_str) > 50:
                    input_str = input_str[:47] + "..."

        # Add row to table
        table.add_row(field, error_type, message, input_str)

    # Create a summary text
    error_count = exc.error_count()
    summary = Text()
    summary.append(f"Found ", style="white")
    summary.append(f"{error_count}", style="bold red")
    summary.append(f" validation error{'s' if error_count != 1 else ''}", style="white")
    if context:
        summary.append(f" in {context}", style="white")

    # Print everything
    console.print()
    console.print(Panel(summary, border_style="red", padding=(0, 2)))
    console.print(table)
    console.print()


if __name__ == "__main__":  # pragma: no cover
    # Example usage
    from pydantic import BaseModel, Field

    class ExampleModel(BaseModel):
        name: str = Field(..., min_length=3)
        age: int = Field(..., ge=0)
        email: str

    try:
        ExampleModel(name="Jo", age=-5)
    except ValidationError as ve:
        pretty_print_pydantic_validation_error(
            ve, context="samx device configuration", model=ExampleModel
        )
