"""Command-line interface for srdedupe.

Provides CLI commands for deduplication pipeline.
"""

import importlib.metadata
import sys
from pathlib import Path

import click

__all__ = ["cli"]

try:
    __version__ = importlib.metadata.version("srdedupe")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.1.0"  # Fallback for development


@click.group()
@click.version_option(version=__version__, prog_name="srdedupe")
def cli() -> None:
    """Safe, reproducible deduplication for bibliographic references.

    Use 'srdedupe COMMAND --help' for command-specific help.
    """


@cli.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    required=True,
    help="Output JSONL file path",
)
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    help="Search recursively in subdirectories (for folder input)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def parse(
    input_path: str,
    output: str,
    recursive: bool,
    verbose: bool,
) -> None:
    """Parse bibliographic files to canonical JSONL format.

    INPUT_PATH can be a single file or a folder containing bibliographic files.
    Format is auto-detected from file content.

    Supported formats: RIS (.ris), NBIB/PubMed (.nbib, .txt), BibTeX (.bib),
    Web of Science (.ciw), EndNote Tagged (.enw)

    Examples
    --------
        srdedupe parse references.ris -o output.jsonl
        srdedupe parse data/ -o all_records.jsonl --recursive
    """
    from srdedupe import parse_file, parse_folder, write_jsonl

    input_path_obj = Path(input_path)

    if verbose:
        click.echo(f"Processing: {input_path}", err=True)

    try:
        if input_path_obj.is_file():
            if verbose:
                click.echo(f"Parsing file: {input_path_obj.name}", err=True)
            records = parse_file(input_path_obj)
        elif input_path_obj.is_dir():
            if verbose:
                click.echo(f"Parsing folder: {input_path} (recursive={recursive})", err=True)
            records = parse_folder(input_path_obj, recursive=recursive)
        else:
            click.secho(
                f"Error: {input_path} is neither a file nor a directory",
                fg="red",
                err=True,
            )
            sys.exit(1)

        if verbose:
            click.echo(f"Found {len(records)} records", err=True)
            click.echo(f"Writing to: {output}", err=True)

        write_jsonl(records, output)

        click.secho(f"✓ Successfully wrote {len(records)} records to {output}", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@cli.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="out",
    help="Output directory for results (default: out)",
)
@click.option(
    "--fpr-alpha",
    type=float,
    default=0.01,
    help="Maximum false positive rate (default: 0.01 = 1%)",
)
@click.option(
    "--t-low",
    type=float,
    default=0.3,
    help="Lower threshold for AUTO_KEEP (default: 0.3)",
)
@click.option(
    "--t-high",
    type=float,
    default=None,
    help="Upper threshold for AUTO_DUP (default: computed via Neyman-Pearson)",
)
@click.option(
    "--blockers",
    type=str,
    default="doi,pmid,year_title",
    help="Comma-separated list of blockers (default: doi,pmid,year_title)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def deduplicate(
    input_path: str,
    output_dir: str,
    fpr_alpha: float,
    t_low: float,
    t_high: float | None,
    blockers: str,
    verbose: bool,
) -> None:
    """Run the complete deduplication pipeline on INPUT_PATH.

    INPUT_PATH can be a single file or a folder containing bibliographic files.

    This command executes all 6 stages of the pipeline:
    1. Parse & Normalize
    2. Candidate Generation
    3. Probabilistic Scoring
    4. Three-Way Decision (FPR-first)
    5. Global Clustering
    6. Canonical Merge

    Outputs are written to OUTPUT_DIR with full audit trail.
    Pairs requiring review are identified but require manual inspection.

    Examples
    --------
        srdedupe deduplicate references.ris
        srdedupe deduplicate data/ -o results --fpr-alpha 0.005
        srdedupe deduplicate refs.ris --t-high 0.95 --blockers doi,pmid
    """
    from srdedupe.engine import PipelineConfig, run_pipeline

    input_path_obj = Path(input_path)
    output_dir_obj = Path(output_dir)

    if verbose:
        click.echo("Starting deduplication pipeline...", err=True)
        click.echo(f"  Input: {input_path}", err=True)
        click.echo(f"  Output: {output_dir}", err=True)
        click.echo(f"  FPR α: {fpr_alpha}", err=True)
        click.echo(f"  t_low: {t_low}", err=True)
        if t_high:
            click.echo(f"  t_high: {t_high} (fixed)", err=True)
        else:
            click.echo("  t_high: (computed via Neyman-Pearson)", err=True)

    try:
        blocker_list = [b.strip() for b in blockers.split(",")]

        config = PipelineConfig(
            fpr_alpha=fpr_alpha,
            t_low=t_low,
            t_high=t_high,
            candidate_blockers=blocker_list,
            output_dir=output_dir_obj,
        )

        if verbose:
            click.echo("\nStage 1: Parsing and normalizing...", err=True)

        result = run_pipeline(input_path=input_path_obj, config=config)

        if not result.success:
            click.secho(f"✗ Pipeline failed: {result.error_message}", fg="red", err=True)
            sys.exit(1)

        if verbose:
            click.echo("\n✓ Pipeline completed successfully!", err=True)
            click.echo("\nResults:", err=True)
            click.echo(f"  Total records: {result.total_records}", err=True)
            click.echo(f"  Candidate pairs: {result.total_candidates}", err=True)
            click.echo(f"  Auto-merged duplicates: {result.total_duplicates_auto}", err=True)
            click.echo(f"  Review required: {result.total_review_pairs}", err=True)
            click.echo("\nOutputs:", err=True)
            for name, path in result.output_files.items():
                click.echo(f"  {name}: {path}", err=True)
        else:
            click.secho(
                f"✓ Deduplicated {result.total_records} records "
                f"({result.total_duplicates_auto} auto-merged, "
                f"{result.total_review_pairs} for review)",
                fg="green",
            )

    except Exception as e:
        click.secho(f"✗ Error: {e}", fg="red", err=True)
        if verbose:
            import traceback

            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
