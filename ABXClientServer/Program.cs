using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Xml;
using Newtonsoft.Json;

class Program
{
    static void Main(string[] args)
    {
        ABXClient client = new ABXClient();
        client.StartClient().Wait();
    }
}

class ABXClient
{
    public Task StartClient()
    {
        string serverIp = "127.0.0.1";
        int port = 3000;

        try
        {
            using (TcpClient client = new TcpClient(serverIp, port))
            {
                Console.WriteLine("Connected to server...");
                NetworkStream stream = client.GetStream();

                // Request to stream all packets (callType 1)
                byte[] requestPayload = new byte[] { 1, 0 };
                stream.Write(requestPayload, 0, requestPayload.Length);

                List<PacketData> packets = new List<PacketData>();
                List<int> receivedSequences = new List<int>();

                // Read and parse response packets
                byte[] buffer = new byte[2048];
                int bytesRead;

                while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    int offset = 0;

                    while (offset < bytesRead)
                    {
                        PacketData packet = new PacketData
                        {
                            Symbol = Encoding.ASCII.GetString(buffer, offset, 4),
                            BuySellIndicator = (char)buffer[offset + 4],
                            Quantity = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 5)),
                            Price = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 9)),
                            SequenceNumber = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 13))
                        };

                        packets.Add(packet);
                        receivedSequences.Add(packet.SequenceNumber);

                        Console.WriteLine($"Received packet: {packet.Symbol}, Seq: {packet.SequenceNumber}");

                        offset += 17; // Size of one packet
                    }
                }

                // Find missing sequences
                List<int> missingSequences = FindMissingSequences(receivedSequences);

                foreach (int missingSeq in missingSequences)
                {
                    Console.WriteLine($"Requesting missing packet with Seq: {missingSeq}");
                    
                    Console.WriteLine("Connected to server...");
                    NetworkStream stream2 = client.GetStream();

                    byte[] resendPayload = new byte[] { 2, (byte)missingSeq };
                    stream2.Write(resendPayload, 0, resendPayload.Length);

                    bytesRead = stream2.Read(buffer, 0, buffer.Length);

                    if (bytesRead > 0)
                    {
                        int offset = 0;

                        PacketData packet = new PacketData
                        {
                            Symbol = Encoding.ASCII.GetString(buffer, offset, 4),
                            BuySellIndicator = (char)buffer[offset + 4],
                            Quantity = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 5)),
                            Price = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 9)),
                            SequenceNumber = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, offset + 13))
                        };

                        packets.Add(packet);
                        Console.WriteLine($"Received missing packet: {packet.Symbol}, Seq: {packet.SequenceNumber}");
                    }
                }

                // Write packets to a JSON file
                string json = JsonConvert.SerializeObject(packets, Newtonsoft.Json.Formatting.Indented);
                File.WriteAllText("Output.json", json);

                Console.WriteLine("Data successfully written to output.json");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: " + ex.Message);
        }

        return Task.CompletedTask;
    }

    private List<int> FindMissingSequences(List<int> receivedSequences)
    {
        receivedSequences.Sort();
        List<int> missingSequences = new List<int>();

        for (int i = receivedSequences[0]; i <= receivedSequences[^1]; i++)
        {
            if (!receivedSequences.Contains(i))
            {
                missingSequences.Add(i);
            }
        }

        return missingSequences;
    }
}

class PacketData
{
    public string? Symbol { get; set; }
    public char BuySellIndicator { get; set; }
    public int Quantity { get; set; }
    public int Price { get; set; }
    public int SequenceNumber { get; set; }
}