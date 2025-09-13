class AnyIOSQLiteInternalError(Exception):
    """
    Raised as a fallback when connect() cannot unwind an exception group into
    a single exception.
    """
