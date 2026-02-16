# Contributing

This document outlines the process and guidelines for contributing.

## How to Contribute

### Reporting Issues

- Check existing issues before creating a new one
- Use a clear, descriptive title
- Include Go version, OS, and `cq` version
- Provide minimal reproducible code examples
- Describe expected vs actual behavior

### Suggesting Features

- Open an issue
- Explain the use case and why it's valuable
- Consider how it fits with existing wrappers/patterns
- Discuss API design before implementing

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Add tests for new functionality
5. Run tests and ensure they pass
6. Update documentation, if needed
7. Commit with descriptive messaging
8. Push to your fork and open a pull request

## Development Setup

### Prerequisites

- Go 1.23 or higher
- make (optional, for convenience commands)

### Clone and Setup

```bash
git clone https://github.com/gnikyt/cq.git
cd cq
go mod download
```

### Running Tests

```bash
# Run all tests
make test

# Or use go test directly
go test ./... -v -race -cover

# Run benchmarks
make bench

# Or use go test directly
go test -bench=. -benchmem
```

### Project Structure

```
.
├── README.md           # Main documentation
├── docs/               # Detailed guides
├── example/            # Example applications
├── *.go               # Core implementation
├── *_test.go          # Tests
└── Makefile           # Build commands
```

## Code Guidelines

### Go Conventions

- Follow standard Go formatting (`gofmt`, `goimports`)
- Write idiomatic Go code
- Keep exported APIs minimal and clear
- Use meaningful variable and function names
- Add doc comments for all exported types and functions

### Wrapper Pattern

When adding new wrappers:

1. Follow the `func(Job) Job` signature
2. Compose cleanly with existing wrappers
3. Preserve context and error semantics
4. Document execution order and behavior
5. Add tests covering success, failure, and edge cases

Example:

```go
// WithExample wraps a job with example behavior.
// It does X when Y happens, and Z otherwise.
func WithExample(job Job, param SomeType) Job {
	return func(ctx context.Context) error {
		// Wrapper logic here
		return job(ctx)
	}
}
```

### Testing

- Write tests for new features and bug fixes
- Aim for >90% test coverage
- Include edge cases and error paths
- Use table-driven tests where appropriate
- Test wrapper composition behavior

Example test structure:

```go
func TestWithExample(t *testing.T) {
	t.Run("success_case", func(t *testing.T) {
		// Test implementation
	})
	
	t.Run("error_case", func(t *testing.T) {
		// Test implementation
	})
}
```

### Documentation

- Update `README.md` if changing core APIs
- Add detailed examples to `docs/` for new features
- Include code examples that demonstrate usage
- Document edge cases and caveats in wrapper docs
- Use the standardized subsection format:
  - **What it does**
  - **When to use**
  - **Example**
  - **Caveat**

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass (`make test`)
- [ ] Code is formatted (`gofmt`)
- [ ] New features have tests
- [ ] Documentation is updated
- [ ] Benchmarks added for performance-sensitive code
- [ ] Commit messages are clear and descriptive

### PR Description

Include:
- Summary of changes
- Motivation and context
- Related issue numbers, if applicable
- Breaking changes, if any
- How to test the changes

### Review Process

- We will review your PR
- Address feedback and update as needed
- Once approved, we will merge as squashed

## Code of Conduct

- Be respectful and constructive
- Welcome newcomers and help them contribute
- Focus on what is best for the project
- Show empathy towards other contributors

## Questions?

- Open a discussion in GitHub Discussions
- Ask in an existing issue if relevant
- Reach out to maintainers if needed

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
