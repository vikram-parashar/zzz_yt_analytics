'use client';
import { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts';
import { LINE_COLORS, toMMYY, fromMonthYear, toMonthYear } from '@/lib/utils';
import type { AgentStats } from '@/lib/types';
interface AgentTimelineChartProps {
  agents: AgentStats[];
}
export function AgentTimelineChart({ agents }: AgentTimelineChartProps) {
  const now = new Date();
  const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  const [startMonth, setStartMonth] = useState(toMonthYear(sixMonthsAgo));
  const [endMonth, setEndMonth] = useState(toMonthYear(now));
  const [timelineData, setTimelineData] = useState<any[]>([]);
  const [agentNames, setAgentNames] = useState<string[]>([]);
  useEffect(() => {
    const sd = fromMonthYear(startMonth).toISOString().slice(0, 10);
    const ed = new Date(fromMonthYear(endMonth).getFullYear(), fromMonthYear(endMonth).getMonth() + 1, 0).toISOString().slice(0, 10);
    fetch(`/api/agent-timeline?startDate=${sd}&endDate=${ed}`)
      .then(r => r.json())
      .then((rows: any[]) => {
        const names = [...new Set(rows.map(d => d.agent_name))];
        const byMonth: Record<string, any> = {};
        for (const d of rows) {
          const m = String(d.month).slice(0, 7);
          if (!byMonth[m]) byMonth[m] = { month: m, label: toMMYY(m) };
          byMonth[m][d.agent_name] = Number(d.video_count ?? 0);
        }
        setTimelineData(Object.values(byMonth).sort((a: any, b: any) => a.month.localeCompare(b.month)));
        setAgentNames(names);
      })
      .catch(console.error);
  }, [startMonth, endMonth]);
  return (
    <section>
      <h2 className="text-2xl font-bold mb-4">Agent Popularity &amp; Trends</h2>
      <div className="card bg-base-100 shadow-xl">
        <div className="card-body p-4">
          <div className="flex flex-wrap items-center gap-3">
            <h3 className="card-title text-sm">Top 5 Agents — Videos Published Per Month</h3>
            <div className="flex gap-2 ml-auto items-center">
              <label className="text-xs text-base-content/60">From:</label>
              <input type="month" className="input input-sm input-bordered w-36"
                value={startMonth} onChange={e => setStartMonth(e.target.value)} />
              <label className="text-xs text-base-content/60">To:</label>
              <input type="month" className="input input-sm input-bordered w-36"
                value={endMonth} onChange={e => setEndMonth(e.target.value)} />
            </div>
          </div>
          <div className="h-64 min-h-[256px]">
            {timelineData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={timelineData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
                  <XAxis dataKey="label" tick={{ fontSize: 10 }} stroke="#7f849c" interval={0} angle={-30} textAnchor="end" height={40} />
                  <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" />
                  <Tooltip />
                  <Legend />
                  {agentNames.map((name, i) => (
                    <Line key={name} type="monotone" dataKey={name}
                      stroke={LINE_COLORS[i % LINE_COLORS.length]}
                      dot={false} strokeWidth={2} />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="h-full flex items-center justify-center text-base-content/40 text-sm">
                No agent daily data yet — collection just started
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}