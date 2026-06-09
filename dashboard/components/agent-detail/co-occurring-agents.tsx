'use client';
import Image from 'next/image';
import Link from 'next/link';
import type { AgentStats, CoOccurringAgent } from '@/lib/types';
interface CoOccurringAgentsProps {
  agents: AgentStats[];
  coOccurring: CoOccurringAgent[];
}
export function CoOccurringAgents({ agents, coOccurring }: CoOccurringAgentsProps) {
  return (
    <div className="card bg-base-100 shadow-xl">
      <div className="card-body p-4">
        <h2 className="card-title text-sm">Agents Discussed Most Alongside</h2>
        {coOccurring.length > 0 ? (
          <div className="flex flex-wrap gap-3">
            {coOccurring.map((ca, i) => {
              const caAgent = agents.find(a => a.name === ca.agent_name);
              return (
                <Link key={ca.agent_name}
                  href={`/agent/${encodeURIComponent(ca.agent_name)}`}
                  className="card bg-base-200 shadow-md w-24 hover:shadow-lg transition-shadow no-underline">
                  <div className="card-body p-2 items-center text-center">
                    <div className="avatar">
                      <div className="w-10 h-10 rounded-md">
                        {caAgent?.img ?
                          <Image height={200} width={200} src={caAgent.img} alt={caAgent.name} /> : (
                            <div className="bg-neutral text-neutral-content w-10 h-10 rounded-full flex items-center justify-center">
                              <span className="text-sm font-bold">{ca.agent_name[0]}</span>
                            </div>
                          )}
                      </div>
                    </div>
                    <p className="text-xs font-semibold truncate w-full">{ca.agent_name}</p>
                    <p className="text-xs text-primary font-bold">{ca.co_video_count} videos</p>
                    <span className="badge badge-xs badge-ghost">#{i + 1}</span>
                  </div>
                </Link>
              );
            })}
          </div>
        ) : (
          <p className="text-sm text-base-content/40 text-center py-8">No co-occurring agent data</p>
        )}
      </div>
    </div>
  );
}