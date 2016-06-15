namespace Suave.Sockets

open System
open System.Net
open System.Net.Sockets
open Suave.Utils

/// A port is an unsigned short (uint16) structure
type Port = uint16

type SocketBinding = 
  { ip   : IPAddress
    port : Port }

  member x.endpoint =
    new IPEndPoint(x.ip, int x.port)

  override x.ToString() =
    if x.ip.AddressFamily = AddressFamily.InterNetworkV6 then
      String.Concat [ "["; x.ip.ToString(); "]:"; x.port.ToString() ]
    else
      String.Concat [ x.ip.ToString(); ":"; x.port.ToString() ]

  static member ip_ = Property<SocketBinding,_> (fun x -> x.ip) (fun v x -> { x with ip=v })
  static member port_ = Property<SocketBinding,_> (fun x -> x.port) (fun v x -> { x with port=v })

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SocketBinding =

  let mk ip port =
    { ip = ip; port = port }

type SocketBindingRange =
  { ip : IPAddress
    firstPort : Port
    lastPort : Port
    defaultPort : Port option
  }

  member x.first =
    let startPort =
      match x.defaultPort with
      | Some p -> p
      | None -> ThreadSafeRandom.next (int x.firstPort) (1 + int x.lastPort) |> uint16
    SocketBinding.mk x.ip startPort

  member x.bindings =
    let startPort = x.first.port
    let startToLast = seq { startPort .. x.lastPort }
    let firstToStart = seq { x.firstPort .. startPort-1us }
    Seq.append startToLast firstToStart
    |> Seq.map (SocketBinding.mk x.ip)

  static member ip_ = Property<SocketBindingRange,_> (fun x -> x.ip) (fun v x -> { x with ip=v })

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SocketBindingRange =

  let mkRange ip defaultPort firstPort lastPort =
    { ip = ip; defaultPort = defaultPort; firstPort = firstPort; lastPort = lastPort }

  let mkSeqRange ip defaultPort firstPort lastPort =
    mkRange ip (Some defaultPort) firstPort lastPort

  let mkRandRange ip defaultPort firstPort lastPort =
    mkRange ip None firstPort lastPort

  let mk ip port =
    mkSeqRange ip port port port
