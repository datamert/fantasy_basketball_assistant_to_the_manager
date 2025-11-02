[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "players_injury_tracking_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared players_injury_tracking = let
  Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("NY5BCoMwEEWvErLpJpew2FIEobRFoeIi6tCmxgxMkmJ6+mqiy3n8ef83Da+V1kpOFo1gT4WGt6LhpZwhCPYIBBYiOXqngQQr1DSFSM44C5bDIZO0fRX4NtFTSA0J5apSpgfzwyWLxiXZFUE7veZG7CLZVwhWShojyoZ43x18N9nFLwJKpWag5DoNL+hx6kCwqkjjPZEMa+yDfq+swbqOEEfBbt5a0Jq37R8=", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [PlayerName = _t])
in
  Source;
shared players_injury_tracking_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "66503be7-cb73-4bf0-b14c-dfcdd90f8f13"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "3360992c-6f78-4136-83fb-402828ede5d7"]}[Data],
  TableNavigation = Navigation_2{[Id = "players_to_track_injuries", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
