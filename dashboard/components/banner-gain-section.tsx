'use client';
import { useState, useEffect, useMemo, useRef } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { toDateStr, fmt } from '@/lib/utils';
import type { AgentStats } from '@/lib/types';
interface BannerPatch {
  banner_start: string;
  banner_end: string;
  label: string;
}
interface DimPatch {
  banner_start: string;
  banner_end: string;
  label: string;
  agents: string[];
}
interface BannerGainRow {
  agent_name: string;
  view_gain: number | null;
}
interface BannerGainSectionProps {
  agents: AgentStats[];
  patches: DimPatch[];
}
export function BannerGainSection({
  agents,
  patches,
}: BannerGainSectionProps) {
  const [selectedPatch, setSelectedPatch] =
    useState<BannerPatch | null>(null);
  const [bannerGainData, setBannerGainData] = useState<
    BannerGainRow[]
  >([]);
  const [queryTime, setQueryTime] = useState<number | null>(null);
  const bannerInitialized = useRef(false);
  useEffect(() => {
    if (
      patches.length > 0 &&
      !bannerInitialized.current
    ) {
      bannerInitialized.current = true;
      setSelectedPatch(
        patches[patches.length - 1]
      );
    }
  }, [patches]);
  useEffect(() => {
    if (!selectedPatch) return;
    const start = new Date(selectedPatch.banner_start)
      .toISOString()
      .split('T')[0];
    const end = new Date(selectedPatch.banner_end)
      .toISOString()
      .split('T')[0];
    fetch(
      `/api/banner-gain?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`
    ).then(r => r.json())
      .then((res: any) => {
        setBannerGainData(res.data ?? res);
        setQueryTime(res.queryTime ?? null);
      })
      .catch(console.error);
  }, [selectedPatch]);
  const selectedBannerData = useMemo(() => {
    const seen = new Set<string>();
    if (!bannerGainData) return [];
    return bannerGainData.sort(
      (a, b) =>
        (b.view_gain ?? 0) - (a.view_gain ?? 0)
    )
      .filter(bg => {
        if (seen.has(bg.agent_name)) return false;
        seen.add(bg.agent_name);
        return true;
      })
      .slice(0, 5)
      .map(bg => ({
        name: bg.agent_name,
        gain: Number(bg.view_gain ?? 0),
        img:
          agents.find(a => a.name === bg.agent_name)
            ?.img || '',
      }));
  }, [bannerGainData, agents]);
  return (
    <section>
      <h2 className="text-2xl font-bold mb-2">
        Agents with Sudden Popularity in Banners
      </h2>
      {queryTime !== null && (
        <p className="text-xs text-base-content/50 mb-2">
          Query took {queryTime.toFixed(3)}s
        </p>
      )}
      {patches.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-4">
          {patches.map(patch => (
            <button
              key={`${patch.banner_start}_${patch.banner_end}`}
              className={`btn btn-sm ${selectedPatch?.banner_start ===
                patch.banner_start &&
                selectedPatch?.banner_end ===
                patch.banner_end
                ? 'btn-primary'
                : 'btn-ghost'
                }`}
              onClick={() => setSelectedPatch(patch)}
            >
              {patch.label}
            </button>
          ))}
        </div>
      )}
      <div className="card bg-base-100 shadow-xl">
        <div className="card-body p-4">
          {selectedPatch && (
            <p className="text-sm text-base-content/60 mb-2">
              Banner period:{' '}
              {toDateStr(selectedPatch.banner_start)}
              {' → '}
              {toDateStr(selectedPatch.banner_end)}
            </p>
          )}
          {selectedBannerData.length > 0 ? (
            <>
              <div className="h-64 min-h-[256px]">
                <ResponsiveContainer
                  width="100%"
                  height="100%"
                >
                  <BarChart
                    data={selectedBannerData}
                    barCategoryGap="20%"
                  >
                    <CartesianGrid
                      strokeDasharray="3 3"
                      stroke="#bdae93"
                    />
                    <XAxis
                      dataKey="name"
                      tick={{ fontSize: 11 }}
                      stroke="#7c6f64"
                    />
                    <YAxis
                      tick={{ fontSize: 10 }}
                      stroke="#7c6f64"
                      tickFormatter={v => fmt(v)}
                    />
                    <Tooltip
                      formatter={(v: any) => [
                        `${fmt(Number(v))} views gained`,
                        'View Gain',
                      ]}
                    />
                    <Bar
                      dataKey="gain"
                      fill="#8f3f71"
                      radius={[4, 4, 0, 0]}
                      name="View Gain"
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              <div className="flex flex-wrap gap-4 mt-4 justify-around pl-[4rem]">
                {selectedBannerData.map((d, i) => (
                  <a
                    key={d.name}
                    href={`/agent/${encodeURIComponent(
                      d.name
                    )}`}
                    className="card bg-base-200 shadow-md w-28 hover:shadow-lg transition-shadow no-underline"
                  >
                    <div className="card-body p-2 items-center text-center">
                      <div className="avatar">
                        <div className="w-12 h-12 rounded-full">
                          {d.img ? (
                            <img
                              src={d.img}
                              alt={d.name}
                            />
                          ) : (
                            <div className="bg-neutral text-neutral-content w-12 h-12 rounded-full flex items-center justify-center">
                              <span className="text-lg font-bold">
                                {d.name[0]}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                      <p className="text-xs font-semibold truncate w-full">
                        {d.name}
                      </p>
                      <p className="text-xs text-primary font-bold">
                        +{fmt(d.gain)}
                      </p>
                      <span className="badge badge-xs badge-ghost">
                        #{i + 1}
                      </span>
                    </div>
                  </a>
                ))}
              </div>
            </>
          ) : (
            <div className="h-32 flex items-center justify-center text-base-content/40 text-sm">
              No agent daily data for this banner period yet
            </div>
          )}
        </div>
      </div>
    </section>
  );
}