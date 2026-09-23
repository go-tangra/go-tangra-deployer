import { expect, test, type Page } from '@playwright/test'
import { base, signIn } from '../../../../gateway/shell/tests/e2e/helpers'

// Quickstart §4 flow for the deployer remote at the three reference widths.
// Needs a full platform; skips without operator credentials.
const password = process.env.E2E_OPERATOR_PASSWORD ?? ''
const email = process.env.E2E_OPERATOR_EMAIL ?? 'ops@example.org'
const viewports = [{ name: 'phone', width: 320, height: 640 }, { name: 'tablet', width: 768, height: 1024 }, { name: 'desktop', width: 1280, height: 800 }]

async function openNav(page: Page, group: string, entry: string): Promise<void> {
  const burger = page.getByRole('button', { name: 'Open navigation' })
  if (await burger.isVisible()) await burger.click()
  const g = page.getByTestId('nav-group-' + group)
  if ((await g.getAttribute('aria-expanded')) !== 'true') await g.click()
  await page.getByTestId('nav-' + group).filter({ hasText: entry }).first().click()
}

test.describe('deployer remote', () => {
  test.skip(!password, 'E2E_OPERATOR_PASSWORD not set')
  for (const vp of viewports) {
    test(`${vp.name}: configuration drawer (credentials write-only), target drawer, jobs`, async ({ page }) => {
      await page.setViewportSize({ width: vp.width, height: vp.height })
      const violations: string[] = []
      await page.addInitScript(() => document.addEventListener('securitypolicyviolation', (e) => console.error('CSP:' + (e as SecurityPolicyViolationEvent).violatedDirective)))
      page.on('console', (m) => { if (m.text().startsWith('CSP:')) violations.push(m.text()) })
      await page.goto(base + '/')
      await signIn(page, email, password)
      await openNav(page, 'deployer', 'Configurations')
      await page.getByTestId('config-new').click()
      const drawer = page.locator('aside[role=dialog]')
      await drawer.getByTestId('config-save').click()
      await expect(drawer.getByRole('alert').first()).toContainText('required')
      const creds = drawer.locator('textarea[data-field=credentials]')
      await creds.fill('{"private_key":"e2e-secret-value"}')
      await creds.fill('')
      await page.keyboard.press('Escape')
      await openNav(page, 'deployer', 'Targets')
      await page.getByTestId('target-new').click()
      await page.locator('aside[role=dialog]').getByTestId('target-save').click()
      await expect(page.locator('aside[role=dialog]').getByRole('alert').first()).toContainText('required')
      await page.keyboard.press('Escape')
      await openNav(page, 'deployer', 'Jobs')
      await expect(page.getByTestId('jobs-table')).toBeVisible()
      await openNav(page, 'deployer', 'Dashboard')
      await expect(page.locator('.stat-tile').first()).toBeVisible()
      expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBeLessThanOrEqual(0)
      expect(await page.locator('main [style]').count()).toBe(0)
      expect(await page.content()).not.toContain('e2e-secret-value')
      expect(violations).toEqual([])
    })
  }
})
