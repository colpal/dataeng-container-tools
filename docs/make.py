#!/usr/bin/env python3
"""Build documentation for DE Container Tools in various formats.

This script builds the Sphinx documentation in HTML and PDF formats.
Python version of Makefile
"""

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Sphinx Docs Generator")

DOCS_PATH = Path(__file__).parent / "src"


def clean_build_dir(build_dir: Path) -> None:
    """Remove the build directory for a clean build."""
    if build_dir.exists():
        logger.info("Cleaning build directory...")
        try:
            shutil.rmtree(build_dir)
            logger.info("Build directory removed successfully")
        except Exception:
            logger.exception("Error removing build directory")


def build_html_docs(build_dir: Path) -> bool:
    """Build HTML documentation."""
    try:
        cmd = ["sphinx-build", "-b", "html", ".", str(build_dir / "html")]
        subprocess.run(cmd, cwd=DOCS_PATH, check=True)
        logger.info("HTML documentation built successfully")
    except subprocess.CalledProcessError:
        logger.exception("Error building HTML documentation")
        return False
    return True


def build_multiversion_docs(build_dir: Path) -> bool:
    """Build multi-version HTML documentation."""
    try:
        cmd = ["sphinx-multiversion", ".", str(build_dir / "html")]
        subprocess.run(cmd, cwd=DOCS_PATH, check=True)
        logger.info("Multi-version documentation built successfully")

        update_switcher_json()

    except subprocess.CalledProcessError:
        logger.exception("Error building multi-version documentation")
        return False
    return True


def update_switcher_json() -> None:
    """Update the switcher.json file with available versions."""
    switcher_file = DOCS_PATH / "_static" / "switcher.json"

    # Basic switcher configuration - can be enhanced to dynamically detect versions
    switcher_data = [
        {
            "name": "v1.0.0",
            "version": "v1.0.0",
            "url": "../../v1.0.0/",
        },
        {
            "name": "v1.0.1",
            "version": "v1.0.1",
            "url": "../../v1.0.1/",
        },
        {
            "name": "v1.0.1",
            "version": "latest",
            "url": "../../v1.0.1/",
        },
    ]

    try:
        with switcher_file.open("w") as f:
            json.dump(switcher_data, f, indent=2)
        logger.info("Updated switcher.json")
    except Exception:
        logger.exception("Error updating switcher.json")


def build_pdf_docs(build_dir: Path) -> bool:
    """Build PDF documentation using LaTeX."""
    try:
        # First build latex files
        cmd = ["sphinx-build", "-b", "latex", ".", str(build_dir / "latex")]
        subprocess.run(cmd, cwd=DOCS_PATH, check=True)

        # Check if system has pdflatex
        try:
            cmd = ["pdflatex", "--version"]
            subprocess.run(cmd, check=True, capture_output=True)
            has_pdflatex = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            has_pdflatex = False

        if has_pdflatex:
            # Then compile PDF
            cmd = ["make"]
            subprocess.run(cmd, cwd=build_dir / "latex", check=True)
            logger.info("PDF documentation built successfully")
            return True

        logger.warning("pdflatex not found. PDF generation requires LaTeX to be installed on your system.")
        logger.info(
            "On Ubuntu/Debian systems, you can install it with: "
            "'sudo apt-get install texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk'",
        )
        logger.info("On Windows, you can install MiKTeX (https://miktex.org/)")
        logger.info("On macOS, you can install MacTeX (https://www.tug.org/mactex/)")

    except subprocess.CalledProcessError:
        logger.exception("Error building PDF documentation")
        return False

    return False


def write_root_index(build_dir: Path, versions: list, template_path: Path) -> None:
    """Write a root index.html that redirects to the latest documentation version using a template."""
    index_path = build_dir / "html" / "index.html"
    # Try to find the latest version (prefer one named 'latest' or 'stable', else use the last entry)
    latest = None
    for v in versions:
        if v.get("version", "").lower() == "latest":
            latest = v
            break
    if not latest and versions:
        latest = versions[-1]

    if latest:
        url = latest.get("url", "").removeprefix("../")
        if template_path.exists():
            html = template_path.read_text(encoding="utf-8").replace("{{ redirect_url }}", f"{url}index.html")
        else:
            msg = f"Template file {template_path} not found for root index.html generation."
            raise FileNotFoundError(msg)
        index_path.write_text(html, encoding="utf-8")
    else:
        msg = "No valid latest version found"
        raise ValueError(msg)


def main() -> None:
    """Run the main build and publish process."""
    logger.info("Building DE Container Tools documentation...")

    parser = argparse.ArgumentParser(description="Build Sphinx documentation in various formats.")
    parser.add_argument("--html", action="store_true", help="Generate HTML documentation (default)")
    parser.add_argument("--multiversion", action="store_true", help="Generate multi-version HTML documentation")
    parser.add_argument("--pdf", action="store_true", help="Generate PDF documentation")
    parser.add_argument("--all", action="store_true", help="Generate all formats (HTML, multi-version, and PDF)")
    parser.add_argument("--build-dir", default="build", help="Build directory (default: build)")
    args = parser.parse_args()

    build_dir = DOCS_PATH / args.build_dir

    # Default to HTML if no flags
    build_html = args.html or not (args.pdf or args.all or args.multiversion)
    build_pdf = args.pdf
    build_multiversion = args.multiversion
    build_all = args.all

    # If --all is specified, build everything
    if build_all:
        build_pdf = True
        build_html = True
        build_multiversion = True

    # Require at least one output format
    if not build_html and not build_pdf and not build_multiversion and not build_all:
        logger.error("Error: You must specify at least one output format:")
        logger.info("  --html           Generate HTML documentation (default)")
        logger.info("  --multiversion   Generate multi-version HTML documentation")
        logger.info("  --pdf            Generate PDF documentation")
        logger.info("  --all            Generate all formats (HTML, multi-version, and PDF)")
        logger.info("\nExample: python make.py --multiversion")
        sys.exit(1)

    # Clean the build directory before building
    clean_build_dir(build_dir)

    # Build HTML docs if requested
    html_success = None
    if build_html:
        html_success = build_html_docs(build_dir)

    # Build multi-version docs if requested
    multiversion_success = None
    if build_multiversion:
        multiversion_success = build_multiversion_docs(build_dir)

    # Build PDF docs if requested
    pdf_success = None
    if build_pdf:
        logger.info("\nGenerating PDF documentation...")
        pdf_success = build_pdf_docs(build_dir)

    # Print summary
    logger.info("\n=== Documentation Build Summary ===")

    if html_success is not None:
        if html_success:
            html_path = build_dir / "html" / "index.html"
            logger.info("\u2713 HTML documentation: %s", html_path.absolute())
        else:
            logger.error("\u2717 HTML documentation build failed")

    if multiversion_success is not None:
        if multiversion_success:
            html_path = build_dir / "html" / "index.html"
            logger.info("\u2713 Multi-version documentation: %s", html_path.absolute())
        else:
            logger.error("\u2717 Multi-version documentation build failed")

    if pdf_success is not None:
        if pdf_success:
            pdf_path = list((build_dir / "latex").glob("*.pdf"))
            if pdf_path:
                logger.info("\u2713 PDF documentation: %s", pdf_path[0].absolute())
            else:
                logger.info("\u2713 PDF files generated in latex directory, but final PDF not found")
        else:
            logger.error("\u2717 PDF documentation build failed")

    # Write root index.html for version switching
    switcher_file = DOCS_PATH / "_static" / "switcher.json"
    template_path = Path(__file__).parent / "root.html"
    versions = []
    if switcher_file.exists():
        try:
            with switcher_file.open() as f:
                versions = json.load(f)
        except Exception:
            logger.exception("Could not read switcher.json for root index.html generation.")
    if versions:
        write_root_index(build_dir, versions, template_path)


if __name__ == "__main__":
    main()
