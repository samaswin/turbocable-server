# Security Policy

## Supported versions

Only the latest release receives security fixes. Older versions are not
backported.

| Version | Supported |
|---------|-----------|
| 0.5.x (current) | Yes |
| < 0.5.0 | No |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please use [GitHub Security Advisories](https://github.com/samaswin/turbocable-server/security/advisories/new)
to report a vulnerability privately. You will receive a response within 5
business days.

Include as much detail as possible:

- Description of the issue and potential impact
- Steps to reproduce or a proof-of-concept (if available)
- Affected versions
- Any suggested mitigations

## Disclosure policy

- Vulnerabilities are triaged within 5 business days of receipt.
- A fix is prepared on a private branch and reviewed before disclosure.
- A GitHub Security Advisory is published once the fix is released, crediting
  the reporter (unless they prefer to remain anonymous).
- The disclosure timeline targets 90 days from initial report; critical issues
  may be disclosed sooner after coordination with the reporter.

## Scope

In scope: the `turbocable-server` binary and its dependencies as declared in
`Cargo.toml`. Out of scope: the NATS server itself, infrastructure choices made
by operators, and third-party clients.
