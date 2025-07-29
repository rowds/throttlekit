# Contributing to throttlekit

We welcome contributions! Here's how to get started:

1. **Fork and clone** the repository
2. **Install dependencies**: `uv sync`
3. **Add tests** in the `tests/` directory
4. **Ensure all tests pass**: `pytest`
5. **Submit a pull request** 🚀

## 🛠️ Developer Setup

To set up the project for development:

1. **Clone the repository**:

   ```bash
   git clone https://github.com/rowds/throttlekit.git
   cd throttlekit
   ```

2. **Install dependencies**:

   ```bash
   uv sync
   ```

This will install all dependencies including development tools and testing frameworks.

## 🧪 Testing

Run tests with coverage:

```bash
pytest --cov=src/throttlekit --cov-report=term-missing
