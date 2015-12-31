#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
arctic_updater

Update data into Arctic data store
"""


def add(a, b):
    """
    Example add function with docstring and doctest

    Parameters
    ----------
    a : int
        Number
    b : int
        Number

    Returns
    -------
    result : sum of ``a`` and ``b``


    >>> add(3, 2)
    5
    """
    return a + b

def main():
    import doctest
    doctest.testmod()

if __name__ == '__main__':
    main()
