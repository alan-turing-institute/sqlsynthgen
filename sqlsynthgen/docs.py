import typer

app = typer.Typer()

@app.command()
def print_int(
    phone: int
) -> None:
    """
    Print argument on screen

    :param message: Text to be printed
    :type message: str
    :raise TypeError: If the phone is invalid.
    :return: None
    :rtype: None

    """
    if type(phone) is int:
        print("My phone: [{}]".format(phone))
    else:
        raise TypeError("Argument `phone` has to be of type integer")