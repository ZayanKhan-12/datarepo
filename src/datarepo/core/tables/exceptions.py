"""Exceptions raised by datarepo itself, rather than by an underlying engine."""


class DatasourceNotAvailable(FileNotFoundError):
    """A table resolved to a location that holds no data.

    Reading a partition that does not exist is an ordinary thing to do — a
    caller asking for yesterday before yesterday landed is not a bug — but the
    query engine reports it as a low-level failure with no mention of the
    table. Polars raises one of three things depending on how the location is
    empty, and one of them is a Rust panic that ``except Exception`` cannot
    catch at all.

    This is raised instead, so the condition can be handled by name:

    >>> try:
    ...     df = catalog.db("nl").events(filters=[...]).collect()
    ... except DatasourceNotAvailable:
    ...     df = None

    It subclasses :class:`FileNotFoundError` because that is what polars
    already raises for the most common case, a partition directory that is not
    there. Code that catches ``FileNotFoundError`` today keeps working.
    """

    def __init__(self, table_name: str, uri: str, reason: str) -> None:
        self.table_name = table_name
        self.uri = uri
        self.reason = reason
        super().__init__(
            f"table {table_name!r} has no data at {uri!r} "
            f"(the query engine reported: {reason})"
        )
