'use client';
import { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceArea,
} from 'recharts';
import { ATTR_COLORS, toMMYY, toDateStr, fromMonthYear, toMonthYear } from '@/lib/utils';
import type { AgentBannerPeriod } from '@/lib/types';
interface VideoTimelineChartProps {
  agentName: string;
  attribute: string;
  initialData: { month: string; video_cnt: number }[];
  banners: AgentBannerPeriod[];
  queryTime?: number;
}
export function VideoTimelineChart({ agentName, attribute, initialData, banners, queryTime }: VideoTimelineChartProps) {
  const now = new Date();
  const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, 1);
  const [startMonth, setStartMonth] = useState(toMonthYear(sixMonthsAgo));
  const [endMonth, setEndMonth] = useState(toMonthYear(now));
  const [timelineData, setTimelineData] = useState(initialData);
  useEffect(() => {
    const sd = fromMonthYear(startMonth).toISOString().slice(0, 10);
    const ed = new Date(fromMonthYear(endMonth).getFullYear(), fromMonthYear(endMonth).getMonth() + 1, 0).toISOString().slice(0, 10);
    fetch(`/api/agent-detail?agentName=${encodeURIComponent(agentName)}&startDate=${sd}&endDate=${ed}`)
      .then(r => r.json())
      .then(data => { if (data.videoTimeline) setTimelineData(data.videoTimeline); })
      .catch(console.error);
  }, [agentName, startMonth, endMonth]);
  const chartData = useMemo(() => {
    return timelineData.map(d => {
      const m = String(d.month).slice(0, 7);
      const [y, mo] = m.split('-');
      return { month: m, label: `${mo}-${y.slice(2)}`, video_count: Number(d.video_cnt ?? 0) };
    });
  }, [timelineData]);
  const formattedBanners = useMemo(() => {
    return banners.map(b => {
      const bs = toDateStr(b.banner_start);
      const be = toDateStr(b.banner_end);
      return { version: b.version, banner_start: bs, banner_end: be, x1: toMMYY(bs.slice(0, 7)), x2: toMMYY(be.slice(0, 7)) };
    });
  }, [banners]);
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="card-title text-sm">Videos Published Per Month</h2>
          {queryTime !== undefined && (
            <span className="text-xs text-base-content/50">Query took {queryTime.toFixed(3)}s</span>
          )}
          <div className="flex gap-2 ml-auto items-center">
            <input type="month" className="input input-sm input-bordered w-36"
              value={startMonth} onChange={e => setStartMonth(e.target.value)} />
            <span className="text-xs">→</span>
            <input type="month" className="input input-sm input-bordered w-36"
              value={endMonth} onChange={e => setEndMonth(e.target.value)} />
          </div>
        </div>
        <div className="h-72 min-h-[288px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#45475a" />
              <XAxis dataKey="label" tick={{ fontSize: 10 }} stroke="#7f849c" interval={0} angle={-30} textAnchor="end" height={40} />
              <YAxis tick={{ fontSize: 10 }} stroke="#7f849c" />
              <Tooltip />
              {formattedBanners.map((b, i) => (
                <ReferenceArea key={i} x1={b.x1} x2={b.x2}
                  fill="#f9e2af" fillOpacity={0.15} stroke="#f9e2af" strokeOpacity={0.4} />
              ))}
              <Line type="monotone" dataKey="video_count"
                stroke={ATTR_COLORS[attribute] || '#7f849c'} dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        {formattedBanners.length > 0 && (
          <div className="flex flex-wrap gap-2 mt-2">
            <span className="text-xs text-base-content/60">Banner periods:</span>
            {formattedBanners.map((b, i) => (
              <span key={i} className="badge badge-warning badge-sm badge-outline">
                v{b.version}: {b.banner_start} → {b.banner_end}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}