#!/usr/bin/env python3
"""
Smoke tests for dataio-artpark package before PyPI release.

This script validates:
1. Package structure and imports
2. CLI functionality
3. SDK functionality
4. Documentation build
5. Basic file integrity
"""

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


def run_command(cmd: List[str], cwd: str = None) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, cwd=cwd, timeout=60
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", "Command timed out"
    except Exception as e:
        return 1, "", str(e)


def test_package_imports():
    """Test that all main package components can be imported."""
    print("🔍 Testing package imports...")

    try:
        # Test main package import
        import dataio

        print(f"✅ Main package imported successfully (version: {dataio.__version__})")

        # Test CLI import
        from dataio.cli import cli  # noqa: F401

        print("✅ CLI module imported successfully")

        # Test SDK import
        from dataio.sdk import DataIOAPI  # noqa: F401

        print("✅ SDK module imported successfully")

        # Test that __all__ is properly defined
        if hasattr(dataio, "__all__"):
            print(f"✅ Package __all__ defined: {dataio.__all__}")
        else:
            print("⚠️  Package __all__ not defined")

        return True

    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during import: {e}")
        return False


def test_cli_functionality():
    """Test CLI commands and help functionality."""
    print("\n🔍 Testing CLI functionality...")

    try:
        # Test main CLI help
        exit_code, stdout, stderr = run_command(["dataio", "--help"])
        if exit_code == 0 and "dataio" in stdout.lower():
            print("✅ Main CLI help command works")
        else:
            print(f"❌ Main CLI help failed: {stderr}")
            return False

        # Test user subcommand help
        exit_code, stdout, stderr = run_command(["dataio", "user", "--help"])
        if exit_code == 0 and "user" in stdout.lower():
            print("✅ User CLI subcommand help works")
        else:
            print(f"❌ User CLI subcommand help failed: {stderr}")
            return False

        # Test that CLI script is properly installed
        exit_code, stdout, stderr = run_command(["which", "dataio"])
        if exit_code == 0:
            print(f"✅ CLI script found at: {stdout.strip()}")
        else:
            print("❌ CLI script not found in PATH")
            return False

        return True

    except Exception as e:
        print(f"❌ CLI test failed: {e}")
        return False


def test_sdk_functionality():
    """Test SDK basic functionality."""
    print("\n🔍 Testing SDK functionality...")

    try:
        from dataio.sdk import DataIOAPI

        # Test SDK class can be instantiated (without API key for basic test)
        # This should raise ValueError due to missing API key, which is expected
        try:
            api = DataIOAPI(base_url="https://test.example.com")
            print("❌ SDK should require API key")
            return False
        except ValueError as e:
            if "API_KEY" in str(e):
                print("✅ SDK properly validates API key requirement")
            else:
                print(f"❌ Unexpected validation error: {e}")
                return False

        # Test SDK with dummy values (should fail gracefully)
        try:
            api = DataIOAPI(base_url="https://test.example.com", api_key="dummy_key")
            print("✅ SDK can be instantiated with dummy values")

            # Test that session is properly configured
            if hasattr(api, "session") and hasattr(api, "base_url"):
                print("✅ SDK session and base_url properly configured")
            else:
                print("❌ SDK missing required attributes")
                return False

        except Exception as e:
            print(f"❌ SDK instantiation failed: {e}")
            return False

        return True

    except Exception as e:
        print(f"❌ SDK test failed: {e}")
        return False


def test_package_structure():
    """Test that package structure is correct."""
    print("\n🔍 Testing package structure...")

    try:
        import dataio

        # Get package path
        package_path = Path(dataio.__file__).parent
        print(f"📁 Package path: {package_path}")

        # Check required directories exist
        required_dirs = ["cli", "sdk"]
        for dir_name in required_dirs:
            dir_path = package_path / dir_name
            if dir_path.exists() and dir_path.is_dir():
                print(f"✅ Required directory exists: {dir_name}")
            else:
                print(f"❌ Required directory missing: {dir_name}")
                return False

        # Check required files exist
        required_files = ["__init__.py", "cli/__init__.py", "sdk/__init__.py"]
        for file_name in required_files:
            file_path = package_path / file_name
            if file_path.exists() and file_path.is_file():
                print(f"✅ Required file exists: {file_name}")
            else:
                print(f"❌ Required file missing: {file_name}")
                return False

        # Check that excluded directories are not present in the package
        excluded_dirs = ["api", "db"]
        for dir_name in excluded_dirs:
            dir_path = package_path / dir_name
            if dir_path.exists():
                print(f"⚠️  Excluded directory still present: {dir_name}")
            else:
                print(f"✅ Excluded directory properly excluded: {dir_name}")

        return True

    except Exception as e:
        print(f"❌ Package structure test failed: {e}")
        return False


def test_documentation_build():
    """Test that documentation can be built."""
    print("\n🔍 Testing documentation build...")

    try:
        # Check if docs directory exists
        docs_dir = Path("docs")
        if not docs_dir.exists():
            print("⚠️  Docs directory not found, skipping documentation test")
            return True

        # Check if we're in an isolated environment (no access to project dependencies)
        # In isolated environments, we can't install docs dependencies
        try:
            # Try to run sphinx-build to see if it's available
            exit_code, stdout, stderr = run_command(["sphinx-build", "--version"])
            if exit_code != 0:
                print(
                    "⚠️  sphinx-build not available in isolated environment, skipping documentation test"
                )
                return True
        except FileNotFoundError:
            print("⚠️  sphinx-build not found, skipping documentation test")
            return True

        # Try to build documentation
        exit_code, stdout, stderr = run_command(
            ["sphinx-build", "-b", "html", "docs/source", "docs/build"]
        )

        if exit_code == 0:
            print("✅ Documentation builds successfully")

            # Check if build output exists
            build_dir = Path("docs/build")
            if build_dir.exists() and (build_dir / "index.html").exists():
                print("✅ Documentation build output exists")
            else:
                print("❌ Documentation build output missing")
                return False

        else:
            print(f"❌ Documentation build failed: {stderr}")
            return False

        return True

    except Exception as e:
        print(f"❌ Documentation test failed: {e}")
        return False


def test_package_metadata():
    """Test package metadata and version."""
    print("\n🔍 Testing package metadata...")

    try:
        import dataio

        # Check version
        if hasattr(dataio, "__version__"):
            version = dataio.__version__
            print(f"✅ Package version: {version}")

            # Basic version format validation
            if version and len(version) > 0:
                print("✅ Version format appears valid")
            else:
                print("❌ Version appears invalid")
                return False
        else:
            print("❌ Package version not found")
            return False

        # Check package name
        try:
            import importlib.metadata

            package_name = importlib.metadata.metadata("dataio-artpark")["Name"]
            print(f"✅ Package name: {package_name}")
        except Exception:
            print("⚠️  Could not retrieve package name from metadata")

        return True

    except Exception as e:
        print(f"❌ Package metadata test failed: {e}")
        return False


def main():
    """Run all smoke tests."""
    print("🚀 Starting dataio-artpark smoke tests...\n")

    tests = [
        ("Package Imports", test_package_imports),
        ("Package Structure", test_package_structure),
        ("Package Metadata", test_package_metadata),
        ("CLI Functionality", test_cli_functionality),
        ("SDK Functionality", test_sdk_functionality),
        ("Documentation Build", test_documentation_build),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 50)
    print("📊 SMOKE TEST SUMMARY")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1

    print(f"\n🎯 Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All smoke tests passed! Package is ready for PyPI release.")
        return 0
    else:
        print("💥 Some smoke tests failed. Please fix issues before releasing.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
