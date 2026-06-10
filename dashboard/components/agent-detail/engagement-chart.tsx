'use client';
import {
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';
import { fmt, toDateStr } from '@/lib/utils';
interface EngagementChartProps {
  data: { date: string; views: number; likes: number }[];
  queryTime?: number;
}
export function EngagementChart({ data, queryTime }: EngagementChartProps) {
  const engagementData = data.map(d => ({
    date: toDateStr(d.date).slice(5),
    views: Number(d.views ?? 0),
    likes: Number(d.likes ?? 0),
  }));
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <h2 className="card-title text-sm">Recent Engagement (Past Month)</h2>
        {queryTime !== undefined && (
          <p className="text-xs text-base-content/50">Query took {queryTime.toFixed(3)}s</p>
        )}
        {engagementData.length > 0 ? (
          <div className="h-48 min-h-[192px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={engagementData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                <XAxis dataKey="date" tick={{ fontSize: 9 }} stroke="#7f849c" />
                <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" tickFormatter={v => fmt(v)} />
                <Tooltip formatter={(v: any) => fmt(Number(v))} />
                <Line type="monotone" dataKey="views" stroke="#cba6f7" dot={false} strokeWidth={2} />
                <Line type="monotone" dataKey="likes" stroke="#a6e3a1" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className="text-sm text-base-content/40 text-center py-8">No daily engagement data yet</p>
        )}
      </div>
    </div>
  );
}