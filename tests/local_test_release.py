#!/usr/bin/env python3
"""
Local release testing script for dataio-artpark.

This script helps you test your package locally before pushing to PyPI.
Run this after building your package with `uv build`.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def run_command(cmd, cwd=None):
    """Run a command and return success status."""
    try:
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Command failed: {' '.join(cmd)}")
            print(f"   Error: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"❌ Command error: {e}")
        return False


def test_build():
    """Test that the package builds successfully."""
    print("🔨 Testing package build...")

    # Clean previous builds
    if Path("dist").exists():
        import shutil

        shutil.rmtree("dist")

    # Build package
    if run_command(["uv", "build"]):
        print("✅ Package built successfully")

        # Check build artifacts exist
        dist_files = list(Path("dist").glob("*"))
        if dist_files:
            print(f"✅ Build artifacts created: {[f.name for f in dist_files]}")
            return True
        else:
            print("❌ No build artifacts found")
            return False
    else:
        print("❌ Package build failed")
        return False


def test_smoke_tests():
    """Run smoke tests on the built package."""
    print("\n🧪 Running smoke tests...")

    # Find the wheel file
    wheel_files = list(Path("dist").glob("*.whl"))
    if not wheel_files:
        print("❌ No wheel file found for testing")
        return False

    wheel_file = wheel_files[0]
    print(f"📦 Testing wheel: {wheel_file.name}")

    # Run smoke tests in isolated environment
    if run_command(
        [
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--with",
            str(wheel_file),
            "python",
            "tests/smoke_test.py",
        ]
    ):
        print("✅ Smoke tests passed")
        return True
    else:
        print("❌ Smoke tests failed")
        return False


def test_install():
    """Test that the package can be installed."""
    print("\n📥 Testing package installation...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a virtual environment
        venv_path = Path(temp_dir) / "test_venv"
        if not run_command(["python", "-m", "venv", str(venv_path)]):
            print("❌ Failed to create test virtual environment")
            return False

        # Find wheel file
        wheel_files = list(Path("dist").glob("*.whl"))
        if not wheel_files:
            print("❌ No wheel file found")
            return False

        wheel_file = wheel_files[0]

        # Install package in test environment
        pip_path = venv_path / ("Scripts" if os.name == "nt" else "bin") / "pip"
        if not run_command([str(pip_path), "install", str(wheel_file)]):
            print("❌ Package installation failed")
            return False

        # Test that package can be imported
        python_path = venv_path / ("Scripts" if os.name == "nt" else "bin") / "python"
        if run_command(
            [
                str(python_path),
                "-c",
                "import dataio; print(f'dataio version: {dataio.__version__}')",
            ]
        ):
            print("✅ Package imports successfully after installation")
            return True
        else:
            print("❌ Package import failed after installation")
            return False


def test_docs():
    """Test that documentation builds."""
    print("\n📚 Testing documentation build...")

    if not Path("docs").exists():
        print("⚠️  No docs directory found, skipping documentation test")
        return True

    # Install docs dependencies
    if not run_command(["uv", "sync", "--group", "docs"]):
        print("❌ Failed to install docs dependencies")
        return False

    # Build documentation
    if run_command(
        ["uv", "run", "sphinx-build", "-b", "html", "docs/source", "docs/build"]
    ):
        print("✅ Documentation builds successfully")
        return True
    else:
        print("❌ Documentation build failed")
        return False


def main():
    """Run all release tests."""
    print("🚀 Starting dataio-artpark release tests...\n")

    tests = [
        ("Build", test_build),
        ("Smoke Tests", test_smoke_tests),
        ("Installation", test_install),
        ("Documentation", test_docs),
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
    print("📊 RELEASE TEST SUMMARY")
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
        print("🎉 All release tests passed! Package is ready for PyPI release.")
        print("\n📋 Next steps:")
        print("1. Commit your changes")
        print(
            "2. Create and push a tag: git tag v0.4.0b11 && git push origin v0.4.0b11"
        )
        print("3. The GitHub workflow will automatically publish to PyPI")
        return 0
    else:
        print("💥 Some release tests failed. Please fix issues before releasing.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
