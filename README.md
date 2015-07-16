# Koopa

Koopa is a [Drake](https://github.com/Factual/drake) to [Luigi](https://github.com/spotify/luigi) converter. 

Both Drake and Luigi are great data-oriented workflow tools. Drake is really great for local development. It's super simple for data scientists and engineers to set up their pipeline using whatever tool makes sense. Luigi, on the other hand, is super powerful with really great Python integration making it great for production pipelines, but less ideal for development. 

Koopa is an implementation of Drake that outputs Luigi code. Developers get all the nice development features of Drake with the added bonus that the output can be easily integrated into production pipelines. 

## Status

This project is under active development is not ready for any real use. 

## Installation

Run `pip install koopa` to install the latest version from PyPI. 

If you prefer to install from source, first `git clone https://github.com/jhorey/koopa.git` and then `python setup.py install`. 

## Getting started

Koopa is designed to be a drop-in replacement for Drake. So by default, Koopa will search for `./Drakefile`. In the same directory, type:

```bash
   $ koopa
```

If you would rather specify the workflow file, type:

```bash
   $ koopa -w /myworkflow/my-workflow.drake
```

## How it works

If you're using Koopa interactively, Koopa will dynamically translate your Drakefile into equivalent Luigi Python scripts. It will then automatically run these Luigi scripts. You can also get Koopa to save the generated Luigi scripts. 

```bash
   $ koopa --luigi-save 
```