# Documentation

This directory contains design documentation and debugging utilities for the simplesingletable library.

## Structure

### `/design-docs`
Design documents and proposals for features and bug fixes. These documents describe the problem, proposed solution, implementation details, and testing approach.

- `blob-field-preservation-fix.md` - Design for fixing blob field metadata preservation in versioned resources

### `/debugging-scripts`
Scripts used for demonstrating issues, debugging, and verifying fixes. These are kept for reference and future debugging needs.

- `demonstrate_blob_issue.py` - Demonstrates the blob field preservation issue with versioned resources
- `debug_blob_versions.py` - Debug script for checking blob version reference functionality

## Adding New Documentation

When working on significant features or bug fixes:

1. Create a design document in `/design-docs` describing the problem and solution
2. Include any demonstration or debugging scripts in `/debugging-scripts`
3. Reference the design doc in pull requests and issues
4. Update this README with brief descriptions of new documents