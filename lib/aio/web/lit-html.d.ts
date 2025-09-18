declare module 'lit-html' {
    export function html(strings: TemplateStringsArray, ...values: unknown[]): unknown;
    export function render(value: unknown, container: Element): void;
    export const nothing: symbol;
}

declare module 'lit-html/directives/repeat.js' {
    export function repeat<T>(
        items: Iterable<T>,
        keyFn: (item: T, index: number) => unknown,
        template: (item: T, index: number) => unknown,
    ): unknown;
}
