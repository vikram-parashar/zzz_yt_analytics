export const ATTR_COLORS: Record<string, string> = {
  Electric: '#cba6f7', Ice: '#89dceb', Fire: '#fab387',
  Physical: '#f38ba8', Ether: '#f5c2e7', Honed_Edge: '#a6e3a1',
};
export const LINE_COLORS = ['#cba6f7', '#fab387', '#a6e3a1', '#f5c2e7', '#89dceb', '#f9e2af', '#f38ba8', '#74c7ec', '#94e2d5', '#b4befe'];
export function toDateStr(v: any): string {
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'string') return v.slice(0, 10);
  return String(v).slice(0, 10);
}
export function fmt(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return n.toString();
}
export function toMonthYear(d: Date): string {
  return d.toISOString().slice(0, 7);
}
export function toMMYY(ym: string): string {
  const [y, m] = ym.split('-');
  return `${m}-${y.slice(2)}`;
}
export function fromMonthYear(my: string): Date {
  const [y, m] = my.split('-').map(Number);
  return new Date(y, m - 1, 1);
}