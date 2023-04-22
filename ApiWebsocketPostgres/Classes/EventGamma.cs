﻿// Event Gamma is Generated by object Alpha
public class EventGamma
{
    public GammaData Data { get; set; }
    public string Type { get; set; }
}

public class GammaData
{
    public string Action { get; set; }
    public string Id { get; set; }
    public GammaParameters Parameters { get; set; }
    public ulong Ticks { get; set; }
    public DateTime Time { get; set; }
    public string Type { get; set; }
}

public class GammaParameters
{
    public string Source { get; set; }
    public string Comment { get; set; }
    public DateTime TimeIso { get; set; }
}

public class GammaNestedJson
{
    public string AlphaId { get; set; } //Which Alpha generated this event
    public string Comment { get; set; }
    public string Visualization { get; set; }
}