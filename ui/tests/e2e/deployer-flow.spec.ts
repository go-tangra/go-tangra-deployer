import { expect, test, type Page } from '@playwright/test'
import { base, signIn } from './helpers'

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
      // Provider first, then its own fields (no JSON text areas).
      await drawer.locator('#provider_type').selectOption('bigip')
      await expect(drawer.locator('[data-provider=bigip]')).toBeVisible()
      expect(await drawer.locator('textarea[data-field=credentials]').count()).toBe(0)
      const secret = drawer.locator('[id="credentials.password"]')
      await secret.fill('e2e-secret-value')
      await secret.fill('')
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

  // T087 / SC-007: a required field the provider marks overridable may be left
  // to the targets; every other rule is enforced in the browser and by the API.
  test('desktop: Cloudflare configuration with a target-supplied zone id, end to end', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 900 })
    await page.goto(base + '/')
    await signIn(page, email, password)
    const csrf = async () => (await page.context().cookies()).find((c) => c.name === '__Host-csrf')?.value ?? ''
    const post = async (path: string, data: unknown) => page.request.post(base + '/api/deployer/v1/' + path, { data, headers: { 'X-CSRF-Token': await csrf() } })
    const zone = '023e105f4ecef8ad9ca31a8372d0c353'
    const stamp = Date.now()
    await openNav(page, 'deployer', 'Configurations')
    const drawer = page.locator('aside[role=dialog]')

    // 1. Through the drawer: a bad zone id and no token are refused in the browser, nothing sent.
    await page.getByTestId('config-new').click()
    await drawer.locator('#provider_type').selectOption('cloudflare')
    await drawer.locator('[id=name]').fill('e2e-cf-' + stamp)
    await drawer.locator('[id="config.zone_id"]').fill('not-a-zone')
    let posted = false
    page.on('request', (r) => { if (r.method() === 'POST' && r.url().endsWith('/api/deployer/v1/configurations')) posted = true })
    await drawer.getByTestId('config-save').click()
    await expect(drawer.locator('[id="config.zone_id"]')).toHaveAttribute('aria-invalid', 'true')
    await expect(drawer.locator('[id="credentials.api_token"]')).toHaveAttribute('aria-invalid', 'true')
    expect(posted).toBe(false)
    await drawer.locator('[id="config.zone_id"]').fill(zone)
    await drawer.locator('[id="credentials.api_token"]').fill('e2e-token-value')
    await drawer.getByTestId('config-save').click()
    await expect(drawer).toBeHidden()

    // 2. The API refuses the same rule independently (422 naming the field).
    const bad = await post('configurations', { name: 'e2e-cf-bad-' + stamp, provider_type: 'cloudflare', config: { zone_id: 'nope' }, credentials: { api_token: 't' } })
    expect(bad.status()).toBe(422)
    expect((await bad.json()).detail.fields['config.zone_id']).toBe('pattern')

    // 3. Edit with a blank token keeps the stored token.
    await page.getByRole('cell', { name: 'e2e-cf-' + stamp }).click()
    await expect(drawer.locator('[id="credentials.api_token"]')).toHaveValue('')
    await drawer.locator('[id=description]').fill('edited')
    await drawer.getByTestId('config-save').click()
    await expect(drawer).toBeHidden()

    // 4. A second configuration without a zone id: saved, warned, badge in the list, Deploy disabled.
    await page.getByTestId('config-new').click()
    await drawer.locator('#provider_type').selectOption('cloudflare')
    await drawer.locator('[id=name]').fill('e2e-cf-shared-' + stamp)
    await drawer.locator('[id="credentials.api_token"]').fill('e2e-token-value')
    await expect(drawer.getByTestId('target-supplied-warning')).toContainText('Zone ID')
    const created = page.waitForResponse((r) => r.request().method() === 'POST' && r.url().endsWith('/api/deployer/v1/configurations'))
    await drawer.getByTestId('config-save').click()
    const sharedId = (await (await created).json()).id as string
    await expect(page.getByTestId('config-row-' + sharedId).getByTestId('needs-target-values')).toHaveText('Needs target values: Zone ID')
    await expect(page.getByTestId('config-deploy-' + sharedId)).toBeDisabled()

    // 5. Attaching it to a target without a zone id: refused in the browser and by the API; with one it attaches.
    await openNav(page, 'deployer', 'Targets')
    await page.getByTestId('target-new').click()
    await drawer.locator('[id=name]').fill('e2e-target-' + stamp)
    await drawer.locator(`[id="cfg-${sharedId}"]`).check()
    await drawer.getByTestId('target-save').click()
    await expect(drawer.locator(`[id="o-${sharedId}.config_overrides.zone_id"]`)).toHaveAttribute('aria-invalid', 'true')
    const t = await post('targets', { name: 'e2e-target-api-' + stamp, auto_deploy: false, certificate_filters: [] })
    const targetId = (await t.json()).id as string
    const refused = await post(`targets/${targetId}/configurations`, { configuration_ids: [sharedId] })
    expect(refused.status()).toBe(422)
    expect((await refused.json()).detail).toMatchObject({ configuration_id: sharedId, fields: { 'config_overrides.zone_id': 'required' } })
    await drawer.locator(`[id="o-${sharedId}.config_overrides.zone_id"]`).fill(zone)
    await drawer.getByTestId('target-save').click()
    await expect(drawer).toBeHidden()
    expect(await page.content()).not.toContain('e2e-token-value')
  })
})
