#!/usr/bin/env node
/**
 * Script to test Genie Space Optimizer app in browser.
 * Navigates to app, captures screenshots, and attempts to start optimization.
 */
import { chromium } from 'playwright';
import { writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';

const APP_URL = 'https://genie-space-optimizer-7474654372024318.aws.databricksapps.com';
const LOGIN_URL = 'https://fe-sandbox-test-genie-accelerator.cloud.databricks.com';
const OUTPUT_DIR = join(process.cwd(), 'browser-test-output');

async function main() {
  mkdirSync(OUTPUT_DIR, { recursive: true });
  const results = { accessible: false, spaces: [], optimizationStarted: false, runId: null, errors: [] };

  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();

  try {
    // Step 1: Navigate to Databricks login first (app redirects there when unauthenticated)
    console.log('Navigating to Databricks login...');
    await page.goto(LOGIN_URL, { waitUntil: 'load', timeout: 45000 });
    await page.waitForTimeout(3000);
    await page.screenshot({ path: join(OUTPUT_DIR, '01-login-page.png'), fullPage: true });

    console.log('Please log in manually in the browser. Waiting 90 seconds...');
    await page.waitForTimeout(90000);

    // Step 2: Navigate to app (should have session now)
    console.log('Navigating to app...');
    try {
      await page.goto(APP_URL, { waitUntil: 'load', timeout: 45000 });
    } catch (e) {
      console.log('App navigation:', e.message);
    }
    await page.waitForTimeout(3000);

    // Screenshot 2: App state after login
    await page.screenshot({ path: join(OUTPUT_DIR, '02-app-dashboard.png'), fullPage: true });
    const initialText = await page.textContent('body');
    console.log('Initial page title:', await page.title());

    // Check if we're still on login page (user may not have logged in)
    const isLoginPage = await page.locator('text=Sign in').count() > 0 ||
      (await page.url()).includes('login') ||
      (await page.url()).includes('accounts');

    if (isLoginPage) {
      results.errors.push('Still on login page after wait - user may need to complete login');
    }

    // Screenshot 3: App state
    await page.screenshot({ path: join(OUTPUT_DIR, '03-app-dashboard.png'), fullPage: true });
    results.accessible = !(await page.locator('text=403').count() > 0);

    // Extract spaces
    const spaceCards = await page.locator('[data-space-id], [data-testid*="space"], .space-card, [class*="space"]').all();
    const spaceLinks = await page.locator('a[href*="/space/"], a[href*="spaceId"]').all();
    for (const link of spaceLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();
      if (href && text?.trim()) results.spaces.push({ name: text.trim(), id: href.match(/[\w-]+$/)?.[0] || href });
    }

    // Fallback: look for any clickable space-like elements
    if (results.spaces.length === 0) {
      const cards = await page.locator('article, [role="article"], [class*="card"]').all();
      for (let i = 0; i < Math.min(cards.length, 5); i++) {
        const text = await cards[i].textContent();
        if (text && text.length > 5 && text.length < 500) {
          results.spaces.push({ name: text.slice(0, 80), id: `card-${i}` });
        }
      }
    }

    // Step 4: Click on first space if available
    const spaceLinkLocator = page.locator('a[href*="/space/"], a[href*="spaceId"]');
    if (await spaceLinkLocator.count() > 0) {
      await spaceLinkLocator.first().click();
      await page.waitForTimeout(2000);
      await page.screenshot({ path: join(OUTPUT_DIR, '04-space-detail.png'), fullPage: true });
    }

    // Step 5: Look for Start Optimization button
    const startBtn = page.locator('button:has-text("Start Optimization"), button:has-text("Start optimization"), [data-testid*="start"], [aria-label*="Start"]');
    const btnCount = await startBtn.count();
    const isDisabled = btnCount > 0 && (await startBtn.first().getAttribute('disabled')) !== null;

    if (btnCount > 0 && !isDisabled) {
      await startBtn.first().click();
      await page.waitForTimeout(3000);
      await page.screenshot({ path: join(OUTPUT_DIR, '05-after-start.png'), fullPage: true });

      // Try to capture run ID from page or network
      const runIdMatch = (await page.textContent('body'))?.match(/run[_\s-]?id[:\s]*([\w-]+)/i);
      const jobRunMatch = (await page.textContent('body'))?.match(/job[_\s-]?run[_\s-]?id[:\s]*([\w-]+)/i);
      if (runIdMatch) results.runId = runIdMatch[1];
      if (jobRunMatch) results.jobRunId = jobRunMatch[1];
      results.optimizationStarted = true;
    } else {
      await page.screenshot({ path: join(OUTPUT_DIR, '05-no-start-button.png'), fullPage: true });
      if (btnCount > 0 && isDisabled) results.errors.push('Start Optimization button is disabled (active run?)');
      else results.errors.push('Start Optimization button not found or not clickable');
    }

    // Final full page text for debugging
    const finalText = await page.textContent('body');
    writeFileSync(join(OUTPUT_DIR, 'page-content.txt'), finalText || '', 'utf8');

  } catch (err) {
    results.errors.push(err.message);
    await page.screenshot({ path: join(OUTPUT_DIR, 'error.png'), fullPage: true }).catch(() => {});
  } finally {
    await browser.close();
  }

  writeFileSync(join(OUTPUT_DIR, 'results.json'), JSON.stringify(results, null, 2), 'utf8');
  console.log('\nResults:', JSON.stringify(results, null, 2));
  console.log('\nScreenshots saved to:', OUTPUT_DIR);
}

main().catch(console.error);
