/// <reference path="./lit-html.d.ts" />
import { html, render, nothing } from 'lit-html';
import { repeat } from 'lit-html/directives/repeat.js';

function obj_map<V>(
    obj: Record<string, V>,
    f: (key: string, value: V) => unknown,
) {
    return repeat(
        Object.entries(obj),
        ([k, _v]) => k,
        ([k, v]) => f(k, v),
    );
}

interface Context {
    status: {
        state: 'pending' | 'success' | 'failure' | 'error';
        description?: string | null;
        target_url?: string | null;
    };
    job?: number | null;
}

interface Pull {
    number: number;
    head: string;
    target_ref: string;
    title: string;
}

interface Branch {
    name: string;
    head: string;
}

interface Commit {
    sha: string;
    why: (string | number)[];
    contexts: Record<string, Context> | null;
}

interface Repository {
    commits: Record<string, Commit>;
    pulls: Record<string, Pull>;
    branches: Record<string, Branch>;
}

interface Forge {
    limits: Record<string, string>;
    repos: Record<string, Repository>;
}

interface Job {
    id: number;
    topic: string;
    priority: number;
    birth: number;
    entry: number;
    worker: number | null;
}

interface Worker {
    id: number;
    name: string;
    topics: string[];
    capacity: number;
    liveness: number;
    jobs: number[];  // job ids
}

interface Brain {
    forges: Record<string, Forge>;
    workers: Record<string, Worker>;
    jobs: Record<string, Job>;
}

// === Pure render functions ===

function state_badge(state: string) {
    const classes: Record<string, string> = {
        success: 'badge-success',
        pending: 'badge-warning',
        failure: 'badge-error',
        error: 'badge-error',
    };
    return classes[state] || 'badge-neutral';
}

function render_context(name: string, context: Context) {
    const { status } = context;
    const content = html`
        <code class="text-xs">${name}</code>
        ${status.description ? html`: ${status.description}` : nothing}
    `;

    return html`
        <span class="badge ${state_badge(status.state)} gap-1">
            ${status.target_url
            ? html`<a href=${status.target_url} class="link">${content}</a>`
            : content}
        </span>
    `;
}

function render_contexts(contexts: Record<string, Context>) {
    return html`
        <tr>
            <td colspan="3" class="bg-base-200">
                <div class="flex flex-wrap gap-1">
                    ${obj_map(contexts, (name, ctx) => render_context(name, ctx))}
                </div>
            </td>
        </tr>
    `;
}

function contexts_summary(contexts: Record<string, Context> | null) {
    if (!contexts) return nothing;

    const states = Object.values(contexts).map(c => c.status.state);
    const success = states.filter(s => s === 'success').length;
    const total = states.length;

    let cls = 'badge-neutral';
    if (total === 0) {
        cls = 'badge-neutral';
    } else if (success === total) {
        cls = 'badge-success';
    } else if (states.some(s => s === 'failure' || s === 'error')) {
        cls = 'badge-error';
    } else {
        cls = 'badge-warning';
    }

    return html`<span class="badge ${cls}">${success}/${total}</span>`;
}

function render_why(repo: Repository, why: (string | number)[]) {
    return why.map(ref => {
        if (typeof ref === 'number') {
            const pull = repo.pulls[String(ref)];
            if (!pull) return html`<span class="badge badge-neutral">#${ref}</span>`;
            return html`
                <span class="badge badge-primary">
                    #${ref}: ${pull.title}
                </span>
            `;
        } else {
            return html`<span class="badge badge-secondary">${ref}</span>`;
        }
    });
}

function render_limits(limits: Record<string, string>) {
    const remaining = limits['remaining'];
    const limit = limits['limit'];
    if (!remaining || !limit) return nothing;

    const pct = Math.round((parseInt(remaining) / parseInt(limit)) * 100);
    const cls = pct > 50 ? 'progress-success' : pct > 20 ? 'progress-warning' : 'progress-error';

    return html`
        <div class="flex items-center gap-2 text-sm">
            <span>API:</span>
            <progress class="progress ${cls} w-24" value=${remaining} max=${limit}></progress>
            <span>${remaining}/${limit}</span>
        </div>
    `;
}

// === Stateful component: repo-viewer ===

class RepoViewer extends HTMLElement {
    private _name = '';
    private _repo: Repository | null = null;
    open_commits = new Set<string>();

    set name(v: string) {
        this._name = v;
        this.update();
    }
    get name() { return this._name; }

    set repo(v: Repository | null) {
        this._repo = v;
        this.update();
    }
    get repo() { return this._repo; }

    private update() {
        if (!this._repo) return;
        render(this.template(), this);
    }

    private toggle(sha: string) {
        if (this.open_commits.has(sha)) {
            this.open_commits.delete(sha);
        } else {
            this.open_commits.add(sha);
        }
        this.update();
    }

    private render_commit(sha: string, commit: Commit) {
        const is_open = this.open_commits.has(sha);

        return html`
            <tr
                class="hover cursor-pointer"
                @click=${() => this.toggle(sha)}
            >
                <td class="font-mono text-sm">${sha.slice(0, 12)}</td>
                <td>
                    <div class="flex flex-wrap gap-1 items-center">
                        ${contexts_summary(commit.contexts)}
                        ${render_why(this._repo!, commit.why)}
                    </div>
                </td>
            </tr>
            ${is_open && commit.contexts ? render_contexts(commit.contexts) : nothing}
        `;
    }

    private template() {
        const commits = Object.entries(this._repo!.commits);
        if (commits.length === 0) return nothing;

        return html`
            <div class="card bg-base-100 shadow-md">
                <div class="card-body p-4">
                    <h2 class="card-title text-lg">${this._name}</h2>
                    <table class="table table-sm">
                        <tbody>
                            ${commits.map(([sha, commit]) => this.render_commit(sha, commit))}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    }
}

customElements.define('repo-viewer', RepoViewer);

// === Top-level app ===

const el = document.querySelector('#app')!;

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

class MonotonicTimestamp extends HTMLElement {
    private _timestamp = 0;
    private _epoch = 0;
    private _interval: number | null = null;

    set timestamp(v: number) { this._timestamp = v; this.update(); }
    set epoch(v: number) { this._epoch = v; this.update(); }

    connectedCallback() {
        this._interval = window.setInterval(() => this.update(), 1000);
        this.update();
    }

    disconnectedCallback() {
        if (this._interval !== null) {
            window.clearInterval(this._interval);
            this._interval = null;
        }
    }

    private update() {
        const server_now = (Date.now() - this._epoch) / 1000;
        const age = server_now - this._timestamp;
        this.textContent = this.format_age(age);
    }

    private format_age(seconds: number): string {
        if (seconds < 0) return 'just now';
        if (seconds < 60) return `${Math.floor(seconds)}s ago`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
        return `${Math.floor(seconds / 3600)}h ago`;
    }
}

customElements.define('monotonic-timestamp', MonotonicTimestamp);

function render_worker(worker: Worker, jobs: Record<string, Job>, server_mono_epoch: number) {
    const assigned = worker.jobs.map(id => jobs[String(id)]).filter(Boolean);

    return html`
        <tr>
            <td class="font-mono">${worker.name}</td>
            <td>
                <div class="flex flex-wrap gap-1">
                    ${worker.topics.map(t => html`<span class="badge badge-sm">${t}</span>`)}
                </div>
            </td>
            <td>${assigned.length}/${worker.capacity}</td>
            <td><monotonic-timestamp .timestamp=${worker.liveness} .epoch=${server_mono_epoch}></monotonic-timestamp></td>
            <td>
                <div class="flex flex-wrap gap-1">
                    ${assigned.map(j => html`<span class="badge badge-primary badge-sm">${j.topic}</span>`)}
                </div>
            </td>
        </tr>
    `;
}

function render_job(job: Job) {
    return html`
        <tr>
            <td class="font-mono">${job.id}</td>
            <td><span class="badge">${job.topic}</span></td>
            <td>${job.priority}</td>
            <td>${job.worker ?? 'queued'}</td>
        </tr>
    `;
}

function render_workers(data: Brain, server_mono_epoch: number) {
    const workers = Object.values(data.workers);

    return html`
        <div class="space-y-4">
            <h1 class="text-2xl font-bold">Workers</h1>
            ${workers.length > 0 ? html`
                <div class="card bg-base-100 shadow-md">
                    <div class="card-body p-4">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Name</th>
                                    <th>Topics</th>
                                    <th>Load</th>
                                    <th>Last Seen</th>
                                    <th>Jobs</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${workers.map(w => render_worker(w, data.jobs, server_mono_epoch))}
                            </tbody>
                        </table>
                    </div>
                </div>
            ` : html`<p class="text-base-content/50">No workers connected</p>`}
        </div>
    `;
}

function render_jobs(data: Brain) {
    const jobs = Object.values(data.jobs);
    const queued = jobs.filter(j => j.worker === null);

    return html`
        <div class="space-y-4">
            <h1 class="text-2xl font-bold">Jobs</h1>
            ${queued.length > 0 ? html`
                <div class="card bg-base-100 shadow-md">
                    <div class="card-body p-4">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>ID</th>
                                    <th>Topic</th>
                                    <th>Priority</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${queued.map(j => render_job(j))}
                            </tbody>
                        </table>
                    </div>
                </div>
            ` : html`<p class="text-base-content/50">No queued jobs</p>`}
        </div>
    `;
}

function render_forge(name: string, forge: Forge) {
    return html`
        <div class="space-y-4">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-bold">${name}</h1>
                ${render_limits(forge.limits)}
            </div>
            <div class="grid gap-4">
                ${obj_map(forge.repos, (repo_name, repo) => html`<repo-viewer .name=${repo_name} .repo=${repo}></repo-viewer>`)
        }
            </div>
        </div>
    `;
}

function render_forges(data: Brain) {
    return html`
        <div class="space-y-4">
            <h1 class="text-2xl font-bold">Revision Control</h1>
            ${obj_map(data.forges, (name, forge) => render_forge(name, forge))}
        </div>
    `;
}

function render_app(data: Brain, server_mono_epoch: number) {
    return html`
        <div class="space-y-8">
            ${render_workers(data, server_mono_epoch)}
            ${render_jobs(data)}
            ${render_forges(data)}
        </div>
    `;
}

function parse_server_mono_epoch(res: Response): number {
    const header = res.headers.get('Server-Monotonic-Now');
    if (!header) return 0;
    const server_mono = parseFloat(header);
    return Date.now() - server_mono * 1000;
}

async function main() {
    let loaded = false;
    let etag: string | null = null;

    render(html`<span class="loading loading-spinner loading-lg"></span>`, el);

    while (true) {
        try {
            const headers: HeadersInit = { 'Prefer': 'wait=30' };
            if (etag) headers['If-None-Match'] = etag;

            const res = await fetch('/api/', { headers });
            const server_mono_epoch = parse_server_mono_epoch(res);

            if (res.status === 304) {
                continue;  // no change, loop immediately
            }

            if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);

            etag = res.headers.get('ETag');
            render(render_app(await res.json(), server_mono_epoch), el);
            loaded = true;
        } catch (e) {
            console.error('Failed to fetch', e);
            if (!loaded) {
                render(html`<div class="alert alert-error">${e}</div>`, el);
            }
            await sleep(1000);  // back off on error
        }
    }
}

main();
