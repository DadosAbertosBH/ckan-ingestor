import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import JobResultPanel from '@/components/JobResultPanel.vue'
import source from '@/components/JobResultPanel.vue?raw'
import type { JobResult } from '@/types'

describe('JobResultPanel reader failures', () => {
  it('renders all reader errors with preserved line breaks', () => {
    const message = 'CsvReader: Jev is disable\nCrawler: resource content is not HTML'
    const result = {
      success: false,
      created_at: '2026-10-02T15:07:00Z',
      error_message: message,
      error_trace: null,
    } as JobResult
    // Install the component stylesheet explicitly: Vite's test transform
    // renders Vue components without injecting their scoped CSS.
    const style = document.createElement('style')
    style.textContent = source.match(/<style scoped>([\s\S]*?)<\/style>/)?.[1] ?? ''
    document.head.appendChild(style)
    const wrapper = mount(JobResultPanel, { props: { result }, attachTo: document.body })
    try {
      const error = wrapper.get('.error-message').element
      expect(error.textContent?.trim()).toBe(message)
      expect(getComputedStyle(error).whiteSpace).toBe('pre-wrap')
    } finally {
      wrapper.unmount()
      style.remove()
    }
  })
})
