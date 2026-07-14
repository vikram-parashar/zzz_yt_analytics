'use client';
import { useState } from 'react';
import Link from 'next/link';
import { fmt } from '@/lib/utils';
import type { AgentMostViewedOn } from '@/lib/types';
interface TopChannelsTableProps {
  channels: AgentMostViewedOn[];
  queryTime?: number;
}
const DEFAULT_VISIBLE = 5;
const STEP = 5;
const MAX_VISIBLE = 40;
export function TopChannelsTable({ channels, queryTime }: TopChannelsTableProps) {
  const [visibleCount, setVisibleCount] = useState(DEFAULT_VISIBLE);
  const cap = Math.min(MAX_VISIBLE, channels.length);
  const canShowMore = visibleCount < cap;
  const canShowLess = visibleCount > DEFAULT_VISIBLE;
  const showMore = () => {
    setVisibleCount(prev => Math.min(prev + STEP, cap));
  };
  const showLess = () => {
    setVisibleCount(prev => Math.max(prev - STEP, DEFAULT_VISIBLE));
  };
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <div className="flex items-center justify-between gap-2">
          <h2 className="card-title text-sm">Top Channels by Views</h2>
          {channels.length > 0 && (
            <span className="text-xs text-base-content/50">
              Showing {Math.min(visibleCount, channels.length)} of {channels.length}
            </span>
          )}
        </div>
        {queryTime !== undefined && (
          <p className="text-xs text-base-content/50">Query took {queryTime.toFixed(3)}s</p>
        )}
        {channels.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="table table-sm">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Channel</th>
                  <th>Total Views</th>
                </tr>
              </thead>
              <tbody>
                {channels.slice(0, visibleCount).map((ch, i) => (
                  <tr key={i}>
                    <td className="text-sm font-bold">{i + 1}</td>
                    <td className="text-sm">
                      <Link href={`https://youtube.com/channel/${ch.channel_id}`} target="_blank" rel="noopener noreferrer" className="link link-hover link-primary">
                        {ch.channel_name}
                      </Link>
                    </td>
                    <td className="text-sm font-bold text-primary">{fmt(ch.total_views ?? 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="flex items-center justify-center gap-2 mt-3">
              {canShowLess && (
                <button
                  className="btn btn-xs btn-ghost"
                  onClick={showLess}
                  type="button"
                >
                  −{STEP}
                </button>
              )}
              {canShowMore && (
                <button
                  className="btn btn-xs btn-primary btn-outline"
                  onClick={showMore}
                  type="button"
                >
                  +{STEP}
                </button>
              )}
              {!canShowMore && visibleCount >= MAX_VISIBLE && channels.length > MAX_VISIBLE && (
                <span className="text-xs text-base-content/40">Max {MAX_VISIBLE} reached</span>
              )}
            </div>
          </div>
        ) : (
          <p className="text-sm text-base-content/40 text-center py-8">No channel data</p>
        )}
      </div>
    </div>
  );
}