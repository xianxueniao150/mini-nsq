package nsqd

import (
	"log"
	"net"
)

type tcpServer struct {
	nsqd  *NSQD
	tcpListener   net.Listener
}


func (tcpServer *tcpServer) serve () error {
	for {
		clientConn, err := tcpServer.tcpListener.Accept()
		if err != nil {
			break
		}
		//每个客户端来连接都起一条协程来处理
		go func() {
			log.Printf("TCP: new client(%s)", clientConn.RemoteAddr())

			prot := &protocol{nsqd: tcpServer.nsqd}

			client := prot.NewClient(clientConn)

			err := prot.IOLoop(client)
			if err != nil {
				log.Printf("client(%s) - %s", clientConn.RemoteAddr(), err)
			}
			client.Close()
		}()
	}

	return nil
}
