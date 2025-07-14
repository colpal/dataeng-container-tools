#!/usr/bin/env python3
"""Build documentation for DE Container Tools in various formats.

This script builds the Sphinx documentation in HTML and PDF formats.
Python version of Makefile
"""

import logging
import shutil
import subprocess
import sys
from pathlib import Path

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Sphinx Docs Generator")


def clean_build_dir() -> None:
    """Remove the build directory for a clean build."""
    docs_dir = Path(__file__).parent
    build_dir = docs_dir / "build"

    if build_dir.exists():
        logger.info("Cleaning build directory...")
        try:
            shutil.rmtree(build_dir)
            logger.info("Build directory removed successfully")
        except Exception:
            logger.exception("Error removing build directory")


def build_html_docs() -> bool:
    """Build HTML documentation."""
    try:
        docs_dir = Path(__file__).parent
        cmd = ["sphinx-build", "-b", "html", ".", "build/html"]
        subprocess.run(cmd, cwd=docs_dir, check=True)
        logger.info("HTML documentation built successfully")
    except subprocess.CalledProcessError:
        logger.exception("Error building HTML documentation")
        return False
    return True


def build_pdf_docs() -> bool:
    """Build PDF documentation using LaTeX."""
    try:
        docs_dir = Path(__file__).parent

        # First build latex files
        cmd = ["sphinx-build", "-b", "latex", ".", "build/latex"]
        subprocess.run(cmd, cwd=docs_dir, check=True)

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
            subprocess.run(cmd, cwd=docs_dir / "build" / "latex", check=True)
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


def main() -> None:
    """Run the main build and publish process."""
    logger.info("Building DE Container Tools documentation...")

    # Parse command line arguments
    build_pdf = "--pdf" in sys.argv
    build_all = "--all" in sys.argv
    build_html = "--html" in sys.argv or not (build_pdf or build_all)  # Default to HTML if no flags

    # If --all is specified, build everything
    if build_all:
        build_pdf = True
        build_html = True

    # Require at least one output format
    if not build_html and not build_pdf and not build_all:
        logger.error("Error: You must specify at least one output format:")
        logger.info("  --html        Generate HTML documentation (default)")
        logger.info("  --pdf         Generate PDF documentation")
        logger.info("  --all         Generate all formats (HTML and PDF)")
        logger.info("\nExample: python build_docs.py --pdf")
        sys.exit(1)

    # Clean the build directory before building
    clean_build_dir()

    # Build HTML docs if requested
    html_success = None
    if build_html:
        html_success = build_html_docs()

    # Build PDF docs if requested
    pdf_success = None
    if build_pdf:
        logger.info("\nGenerating PDF documentation...")
        pdf_success = build_pdf_docs()

    # Print summary
    logger.info("\n=== Documentation Build Summary ===")

    if html_success is not None:
        if html_success:
            docs_dir = Path(__file__).parent
            html_path = docs_dir / "build" / "html" / "index.html"
            logger.info("✓ HTML documentation: %s", html_path.absolute())
        else:
            logger.error("✗ HTML documentation build failed")

    if pdf_success is not None:
        if pdf_success:
            docs_dir = Path(__file__).parent
            pdf_path = list(docs_dir.glob("build/latex/*.pdf"))
            if pdf_path:
                logger.info("✓ PDF documentation: %s", pdf_path[0].absolute())
            else:
                logger.info("✓ PDF files generated in latex directory, but final PDF not found")
        else:
            logger.error("✗ PDF documentation build failed")


if __name__ == "__main__":
    main()
