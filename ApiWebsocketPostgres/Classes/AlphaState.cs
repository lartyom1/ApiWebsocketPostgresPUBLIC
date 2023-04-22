//object Alpha may change it's state, if it's state is changed, Alpha generates a StateChanged event
public class AlphaNestedData
{
    public string Id { get; set; }
    public Parameters Param { get; set; }
    public States States { get; set; }
    public ulong Ticks { get; set; } 
    public DateTime Time { get; set; }
}

public class AlphaData //StateChanged event data
{
    public AlphaNestedData NestedData { get; set; }
    public string Type { get; set; }
    /*
     * --- *
     * --- *
    */
}

public class Parameters
{
    public string ParameterOne { get; set; }
    public string ParameterTwo { get; set; }
    public string ParameterThree { get; set; }
    /*
     * --- *
     * --- *
    */
}

public class States
{
    public bool StateOne { get; set; }
    public bool StateTwo { get; set; }
    public bool StateThree { get; set; }
}