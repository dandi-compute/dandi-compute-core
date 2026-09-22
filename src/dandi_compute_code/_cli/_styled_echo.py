import click
import pydantic


@pydantic.validate_call
def _styled_echo(text: str, color: str) -> None:
    """
    Style a message for Click output.

    Parameters
    ----------
    text : str
        The message text to be styled.
    color : str
        The color to apply to the message (e.g., 'red', 'green', 'yellow', etc.).
    """
    message = click.style(text=text, fg=color)
    click.echo(message=message)
