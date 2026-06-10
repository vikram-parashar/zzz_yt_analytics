export const ATTR_COLORS: Record<string, string[]> = {
  Fire: ['#FE801F', '#F31000'],
  Ice: ['#04BFFA', '#04BFFA'],
  Frost: ['#04BFFA', '#04BFFA'],
  Electric: ['#2DCBFC', '#0378FD'],
  Physical: ['#FE9800', '#DF9730'],
  Honed_Edge: ['#FE9800', '#DF9730'],
  Ether: ['#4B6DEA', '#FF192C'],
  Auric_Ink: ['#4B6DEA', '#FF192C']
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