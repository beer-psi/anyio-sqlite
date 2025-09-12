# anyio-sqlite

[![Python versions](https://img.shields.io/pypi/pyversions/anyio-sqlite.svg)](https://pypi.org/project/anyio-sqlite)
[![PyPI release](https://img.shields.io/pypi/v/anyio-sqlite.svg)](https://pypi.org/project/anyio-sqlite)
[![License](https://img.shields.io/pypi/l/anyio-sqlite.svg)](https://github.com/beer-psi/anyio-sqlite/tree/trunk?tab=readme-ov-file#license)


`anyio-sqlite` is an AnyIO bridge to the standard `sqlite3` database module, providing
async versions of all connection, cursor and blob (Python 3.11+) methods that runs
on both [asyncio](https://docs.python.org/3/library/asyncio.html) and [Trio](https://github.com/python-trio/trio).
Asynchronous context management is also supported.

## Install

`anyio-sqlite` is compatible with Python 3.9 and newer. Install it from PyPI:

```
pip install anyio-sqlite
```

## Examples

Use it the same way as the standard `sqlite3` module, simply scatter the `await` keyword when needed:

```python
import anyio
import anyio_sqlite

async def main():
    async with await anyio_sqlite.connect("example.sqlite3") as con:
        async with await con.cursor() as cur:
            await cur.execute("CREATE TABLE movie(title, year, score)")
            await cur.execute("""
                INSERT INTO movie VALUES
                    ('Monty Python and the Holy Grail', 1975, 8.2),
                    ('And Now for Something Completely Different', 1971, 7.5)
            """)
            await con.commit()

        async with await con.execute("SELECT score FROM movie") as cur:
            async for row in cur:
                ...

anyio.run(main)
```

It can also be used procedurally:

```python
import anyio
import anyio_sqlite

async def main():
    con = await anyio_sqlite.connect("example.sqlite3")
    cur = await con.cursor()

    await cur.execute("CREATE TABLE movie(title, year, score)")
    await cur.execute("""
        INSERT INTO movie VALUES
            ('Monty Python and the Holy Grail', 1975, 8.2),
            ('And Now for Something Completely Different', 1971, 7.5)
    """)
    await con.commit()
    await cur.aclose()

    cur = await con.execute("SELECT score FROM movie")

    async for row in cur:
        ...

    await cur.aclose()
    await con.aclose()

anyio.run(main)
```

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

at your option.
