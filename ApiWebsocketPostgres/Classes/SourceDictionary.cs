//There are multiple sources of Gamma Events, each source contains multiple Alpha objects
public class SourceDictionary
{
    public List<SourceLine> ListData { get; set; }
}

public class SourceLine
{
    public int SourceId { get; set; }
    public string SourceName { get; set; }

}