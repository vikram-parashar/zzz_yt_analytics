import Link from 'next/link';
import { fmt } from '@/lib/utils';
import type { AgentMostViewedOn } from '@/lib/types';
interface TopChannelsTableProps {
  channels: AgentMostViewedOn[];
  queryTime?: number;
}
export function TopChannelsTable({ channels, queryTime }: TopChannelsTableProps) {
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <h2 className="card-title text-sm">Top Channels by Views</h2>
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
                {channels.slice(0, 8).map((ch, i) => (
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
          </div>
        ) : (
          <p className="text-sm text-base-content/40 text-center py-8">No channel data</p>
        )}
      </div>
    </div>
  );
}