"""
Command Line Interface - Modular Version

Provides interactive CLI for newspaper selection and download management.
"""

import click
from .config import Config
from .commands.newspaper import newspaper


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    """Newsagger - Library of Congress News Archive Aggregator"""
    config = Config()
    if verbose:
        config.log_level = 'DEBUG'
    config.setup_logging()


# Register command groups
cli.add_command(newspaper)


if __name__ == '__main__':
    cli()