import { expect, test } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'
import { base, signIn } from './helpers'

// T070 / SC-006: every deployer view inside the shell, both themes, zero serious or
// critical axe findings. Needs a full platform; skips without operator credentials.
const password = process.env.E2E_OPERATOR_PASSWORD ?? ''
const email = process.env.E2E_OPERATOR_EMAIL ?? 'ops@example.org'
const routes = ['/deployer',  '/deployer/targets',  '/deployer/configurations',  '/deployer/jobs']

test.describe('deployer accessibility', () => {
  test.skip(!password, 'E2E_OPERATOR_PASSWORD not set')

  for (const theme of ['freya-light', 'freya-dark']) {
    test(`views are axe clean in ${theme}`, async ({ page }) => {
      await page.addInitScript((t) => localStorage.setItem('freya.theme', t), theme)
      await page.goto(base + '/')
      await signIn(page, email, password)
      for (const route of routes) {
        await page.goto(base + route)
        await expect(page.locator('main h1, main h2').first()).toBeVisible({ timeout: 15_000 })
        expect(await page.evaluate(() => document.documentElement.getAttribute('data-theme'))).toBe(theme)
        const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze()
        const blocking = results.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical')
        expect(blocking, route + ': ' + JSON.stringify(blocking.map((v) => ({ id: v.id, nodes: v.nodes.map((n) => n.target) })))).toEqual([])
        expect(await page.locator('[style]').count(), route + ': no inline styles').toBe(0)
      }
    })
  }

  // T087: the open configuration drawer (provider form) and the target form.
  for (const theme of ['freya-light', 'freya-dark']) {
    test(`configuration drawer and target form are axe clean in ${theme}`, async ({ page }) => {
      await page.addInitScript((t) => localStorage.setItem('freya.theme', t), theme)
      await page.goto(base + '/')
      await signIn(page, email, password)
      const check = async (what: string) => {
        const results = await new AxeBuilder({ page }).include('aside[role=dialog]').withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze()
        const blocking = results.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical')
        expect(blocking, what + ': ' + JSON.stringify(blocking.map((v) => ({ id: v.id, nodes: v.nodes.map((n) => n.target) })))).toEqual([])
      }
      await page.goto(base + '/deployer/configurations')
      await page.getByTestId('config-new').click()
      for (const provider of ['bigip', 'webhook', 'inventory-agent']) {
        await page.locator('aside[role=dialog] #provider_type').selectOption(provider)
        await expect(page.locator(`aside[role=dialog] [data-provider="${provider}"]`)).toBeVisible()
        await check('configuration drawer ' + provider)
      }
      await page.keyboard.press('Escape')
      await page.goto(base + '/deployer/targets')
      await page.getByTestId('target-new').click()
      await page.locator('aside[role=dialog] [data-test=target-configs] input').first().check()
      await check('target form')
    })
  }
})
