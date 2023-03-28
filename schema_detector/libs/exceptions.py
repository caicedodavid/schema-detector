class HeterogeneousListException(Exception):
    """Raised when a non-homogeneous list is encountered."""


class UnidentifiedTypeException(Exception):
    """Raised when an unidentified type is found in one of the records."""


class IncompatibleSchemasException(Exception):
    """Raised when two schemas are incompatible."""
