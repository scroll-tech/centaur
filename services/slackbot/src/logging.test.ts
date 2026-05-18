import { describe, expect, it } from 'bun:test'
import { sanitizeLogString, sanitizeLogValue } from './logging'

describe('Slackbot log sanitization', () => {
  it('redacts nested PII and secrets', () => {
    const sanitized = sanitizeLogValue({
      email: 'alice@example.com',
      userPhone: '+1 (415) 555-1212',
      details: {
        token: 'xoxb-super-secret-token',
        note: 'Email bob@example.com or call 415-555-1212. SSN 123-45-6789.'
      },
      authorization: 'Bearer abc.def.ghi'
    })

    expect(sanitized).toEqual({
      email: '[REDACTED:email]',
      userPhone: '[REDACTED:phone]',
      details: {
        token: '[REDACTED:secret]',
        note: 'Email [REDACTED:email] or call [REDACTED:phone]. SSN [REDACTED:ssn].'
      },
      authorization: '[REDACTED:secret]'
    })
  })

  it('sanitizes error messages without exposing raw values', () => {
    const sanitized = sanitizeLogValue(
      new Error('Slack failed for alice@example.com with Bearer abc.def')
    )

    expect(sanitized).toEqual({
      name: 'Error',
      message: 'Slack failed for [REDACTED:email] with Bearer [REDACTED:secret]',
      cause: undefined
    })
  })

  it('keeps non-sensitive strings readable', () => {
    expect(sanitizeLogString('Processed channel C123 at 2026-05-15T10:00:00Z')).toBe(
      'Processed channel C123 at 2026-05-15T10:00:00Z'
    )
  })
})
