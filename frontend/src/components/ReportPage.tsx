import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

type CustomerTelemetryDaily = {
  event_date: string;
  customer_id: number;
  external_id: string;
  full_name: string;
  signup_channel: string;
  total_sessions: number;
  total_session_time: number;
  avg_session_time: number;
  errors_count: number;
  total_payload_mb: number;
  avg_signal_strength: number;
  updated_at: string;
};

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<CustomerTelemetryDaily[] | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
           Authorization: `Bearer ${keycloak.token}`,
           Accept: 'application/json',
        }
      });
      
      if (!response.ok) {
        const payload = await response.json().catch(() => null);
        const message = payload?.detail || 'Failed to load report';
        throw new Error(message);
      }

      const data: CustomerTelemetryDaily[] = await response.json();
      setReport(data);
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-xl">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>
        
        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
        {report && (
          <div className="mt-6">
            <h2 className="text-xl font-semibold mb-4">Daily Telemetry</h2>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Date
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Customer ID
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      External ID
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Full Name
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Signup Channel
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Total Sessions
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Total Session Time (s)
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Avg Session Time (s)
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Errors
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Total Payload (MB)
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Avg Signal Strength
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Updated At
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {report.map((row) => (
                    <tr key={`${row.event_date}-${row.customer_id}`}>
                      <td className="px-4 py-2 whitespace-nowrap">{row.event_date}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.customer_id}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.external_id}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.full_name}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.signup_channel}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.total_sessions}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.total_session_time}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.avg_session_time.toFixed(2)}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.errors_count}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.total_payload_mb.toFixed(1)}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.avg_signal_strength.toFixed(1)}</td>
                      <td className="px-4 py-2 whitespace-nowrap">{row.updated_at}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;