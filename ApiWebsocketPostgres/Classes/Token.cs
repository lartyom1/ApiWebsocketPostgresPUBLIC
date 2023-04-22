//Websocket authorisation token
public class DataToken
{
    public string Token { get; set; }
}

public class Token
{
    public DataToken Data { get; set; }
    public string Status { get; set; }
}