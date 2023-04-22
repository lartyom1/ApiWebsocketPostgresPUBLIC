//Alpha is an observer
//Object Alpha generates events
//There are mutiple Servers(Sources) with mutiple Alphas connected

public class ObjectAlpha
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string Server { get; set; }
    public Settings Settings { get; set; }
    public Status Status { get; set; }
    public string Url { get; set; }
}

public class Root
{
    public List<ObjectAlpha> Data { get; set; }
}

public class Settings
{
    public string SettingOne { get; set; }
    public string SettingTwo { get; set; }
    public string SettingThree { get; set; }
}

public class Status
{
    public bool Enabled { get; set; }
    public string StatusOne { get; set; }
    public string StatusTwo { get; set; }
    public bool StatusThree { get; set; }
    /*
     * --- *
     * --- *
    */
}