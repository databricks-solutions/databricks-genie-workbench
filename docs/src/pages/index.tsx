import type {ReactNode} from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Button from '@site/src/components/Button';

const CAPABILITIES = [
  {
    title: 'Create',
    body: 'A multi-turn AI agent walks you from business requirements to a fully configured Genie Agent.',
    to: '/docs/features/create-agent',
  },
  {
    title: 'Score',
    body: 'The rule-based IQ Scanner grades Genie Agent quality across 12 checks and assigns a maturity tier.',
    to: '/docs/features/iq-scanner',
  },
  {
    title: 'Optimize',
    body: 'A benchmark-driven pipeline measures real accuracy, diagnoses failures, and iterates to a target.',
    to: '/docs/features/auto-optimize',
  },
  {
    title: 'Track',
    body: 'Every scan, optimization run, and config change is persisted to Lakebase so you can see progress.',
    to: '/docs/platform/operations',
  },
];

const STATS = [
  {value: '12', label: 'IQ quality checks'},
  {value: '3', label: 'maturity tiers'},
  {value: '6', label: 'optimization pipeline tasks'},
  {value: '9', label: 'evaluation judges'},
];

const PERSONAS = [
  {
    title: 'Genie Agent developer',
    body: 'Build and refine Genie Agents. Start with the Create Agent and the IQ Scanner.',
    to: '/docs/getting-started/introduction',
  },
  {
    title: 'Workspace admin',
    body: 'Deploy and operate the Workbench. Start with the Deployment Guide and the auth model.',
    to: '/docs/getting-started/deployment-guide',
  },
  {
    title: 'Contributor',
    body: 'Extend the codebase. Start with the Architecture Overview, then the relevant feature doc.',
    to: '/docs/getting-started/architecture-overview',
  },
];

function Eyebrow({children}: {children: ReactNode}) {
  return (
    <p className="text-sm font-semibold uppercase tracking-wide text-[#2272b4] dark:text-[#4299e0]">
      {children}
    </p>
  );
}

function Hero() {
  const {siteConfig} = useDocusaurusContext();
  const logo = useBaseUrl('img/logo.svg');
  return (
    <header className="w-full bg-gradient-to-b from-[#1b3139] to-[#0f2027] text-white">
      <div className="mx-auto flex max-w-5xl flex-col items-center px-4 py-20 text-center md:px-10 md:py-28">
        <img src={logo} alt="Genie Workbench" className="mb-8 w-20 md:w-24" />
        <span className="mb-5 inline-flex items-center rounded-full border border-white/20 bg-white/5 px-3 py-1 text-xs font-medium uppercase tracking-wide text-slate-200">
          Databricks App
        </span>
        <h1 className="text-4xl font-medium leading-tight md:text-5xl">
          {siteConfig.title}
        </h1>
        <p className="mt-4 max-w-2xl text-lg text-slate-300 md:text-xl">
          {siteConfig.tagline}. A unified developer tool deployed as a
          Databricks App — with on-behalf-of auth, Lakebase persistence, and a
          benchmark-driven optimization pipeline.
        </p>
        <div className="mt-10 flex flex-wrap items-center justify-center gap-4">
          <Button to="/docs/getting-started/introduction" variant="primary">
            Get Started
          </Button>
          <Button to="/docs/getting-started/deployment-guide" variant="navy">
            Deploy
          </Button>
          <Link
            href={`https://github.com/${siteConfig.organizationName}/${siteConfig.projectName}`}
            className="text-slate-200 underline-offset-4 hover:underline">
            View on GitHub →
          </Link>
        </div>
      </div>
    </header>
  );
}

function Stats() {
  return (
    <section className="border-b border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-[#16242a]">
      <div className="mx-auto grid max-w-6xl grid-cols-2 gap-6 px-4 py-12 md:grid-cols-4 md:px-10">
        {STATS.map((s) => (
          <div key={s.label} className="text-center">
            <div className="text-4xl font-semibold text-[#2272b4] dark:text-[#4299e0]">
              {s.value}
            </div>
            <div className="mt-1 text-sm text-slate-600 dark:text-slate-400">
              {s.label}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function Capabilities() {
  return (
    <section className="mx-auto max-w-6xl px-4 py-16 md:px-10">
      <Eyebrow>The continuous-improvement loop</Eyebrow>
      <h2 className="mt-1 text-3xl font-normal">Five capabilities, one workflow</h2>
      <p className="mt-2 max-w-2xl text-slate-600 dark:text-slate-400">
        Enter the loop at any point and repeat as the Genie Agent evolves — after
        optimizing, re-scan to see the updated score.
      </p>
      <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
        {CAPABILITIES.map((item, i) => (
          <Link
            key={item.title}
            to={item.to}
            className="group rounded-xl border border-slate-200 bg-white p-6 no-underline transition hover:border-[#2272b4] hover:shadow-md hover:no-underline dark:border-slate-700 dark:bg-[#1f3239] dark:hover:border-[#4299e0]">
            <div className="flex items-center gap-3">
              <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-[#2272b4]/10 text-sm font-semibold text-[#2272b4] dark:bg-[#4299e0]/15 dark:text-[#4299e0]">
                {i + 1}
              </span>
              <h3 className="m-0 text-xl font-medium text-[#1b3139] group-hover:text-[#2272b4] dark:text-white dark:group-hover:text-[#4299e0]">
                {item.title}
              </h3>
              <span
                aria-hidden="true"
                className="ml-auto text-slate-300 transition group-hover:translate-x-0.5 group-hover:text-[#2272b4] dark:text-slate-600 dark:group-hover:text-[#4299e0]">
                →
              </span>
            </div>
            <p className="mt-3 text-slate-600 dark:text-slate-300">{item.body}</p>
          </Link>
        ))}
      </div>
    </section>
  );
}

function QuickStart() {
  return (
    <section className="border-t border-slate-200 bg-slate-50 dark:border-slate-800 dark:bg-[#16242a]">
      <div className="mx-auto grid max-w-6xl items-center gap-10 px-4 py-16 md:grid-cols-2 md:px-10">
        <div>
          <Eyebrow>Quick start</Eyebrow>
          <h2 className="mt-1 text-3xl font-normal">Deploy in one command</h2>
          <p className="mt-3 text-slate-600 dark:text-slate-300">
            Genie Workbench runs entirely on the Databricks Apps platform. The
            guided installer provisions resources, writes your config, builds the
            frontend, and deploys — no local server required.
          </p>
          <div className="mt-6">
            <Button to="/docs/getting-started/deployment-guide" variant="navy">
              Read the Deployment Guide
            </Button>
          </div>
        </div>
        <div className="overflow-hidden rounded-xl border border-slate-700 bg-[#0f2027] text-sm shadow-lg">
          <div className="flex items-center gap-2 border-b border-slate-700 bg-[#16242a] px-4 py-3">
            <span className="h-3 w-3 rounded-full bg-[#eb1600]/80" />
            <span className="h-3 w-3 rounded-full bg-amber-400/80" />
            <span className="h-3 w-3 rounded-full bg-emerald-400/80" />
            <span className="ml-2 font-mono text-xs text-slate-400">bash</span>
          </div>
          <pre className="m-0 overflow-x-auto bg-transparent p-5 font-mono leading-relaxed text-slate-200">
            <code>
              <span className="text-slate-400"># First-time setup (interactive)</span>
              {'\n'}./scripts/install.sh{'\n\n'}
              <span className="text-slate-400"># Subsequent deploys</span>
              {'\n'}./scripts/deploy.sh{'\n\n'}
              <span className="text-slate-400"># Code-only update (faster)</span>
              {'\n'}./scripts/deploy.sh --update
            </code>
          </pre>
        </div>
      </div>
    </section>
  );
}

function Personas() {
  return (
    <section className="mx-auto max-w-6xl px-4 py-16 md:px-10">
      <Eyebrow>Start here</Eyebrow>
      <h2 className="mt-1 text-3xl font-normal">Choose your path</h2>
      <div className="mt-8 grid grid-cols-1 gap-5 sm:grid-cols-3">
        {PERSONAS.map((item) => (
          <Link
            key={item.title}
            to={item.to}
            className="group rounded-xl border border-slate-200 bg-white p-6 no-underline transition hover:border-[#2272b4] hover:shadow-md hover:no-underline dark:border-slate-700 dark:bg-[#1f3239] dark:hover:border-[#4299e0]">
            <h3 className="flex items-center justify-between text-xl font-medium text-[#1b3139] group-hover:text-[#2272b4] dark:text-white dark:group-hover:text-[#4299e0]">
              {item.title}
              <span
                aria-hidden="true"
                className="text-slate-300 transition group-hover:translate-x-0.5 group-hover:text-[#2272b4] dark:text-slate-600 dark:group-hover:text-[#4299e0]">
                →
              </span>
            </h3>
            <p className="mt-2 text-slate-600 dark:text-slate-300">{item.body}</p>
          </Link>
        ))}
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title="Documentation" description={siteConfig.tagline}>
      <Hero />
      <main>
        <Stats />
        <Capabilities />
        <QuickStart />
        <div className="border-t border-slate-200 dark:border-slate-800">
          <Personas />
        </div>
      </main>
    </Layout>
  );
}
