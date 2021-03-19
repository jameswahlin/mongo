/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <set>

#include "mongo/client/remote_command_targeter.h"

namespace mongo {

class RemoteCommandTargeterMock final : public RemoteCommandTargeter {
public:
    RemoteCommandTargeterMock();
    virtual ~RemoteCommandTargeterMock();

    /**
     * Shortcut for unit-tests.
     */
    static std::shared_ptr<RemoteCommandTargeterMock> get(
        std::shared_ptr<RemoteCommandTargeter> targeter);

    /**
     * Returns the value last set by setConnectionStringReturnValue.
     */
    ConnectionString connectionString() override;

    /**
     * Returns the return value last set by setFindHostReturnValue.
     * Returns ErrorCodes::InternalError if setFindHostReturnValue was never called.
     */
    SemiFuture<HostAndPort> findHost(const ReadPreferenceSetting& readPref,
                                     const CancellationToken& cancelToken) override;

    SemiFuture<std::vector<HostAndPort>> findHosts(const ReadPreferenceSetting& readPref,
                                                   const CancellationToken& cancelToken) override;

    StatusWith<HostAndPort> findHost(OperationContext* opCtx,
                                     const ReadPreferenceSetting& readPref) override;

    /**
     * Adds host to a set of hosts marked down, otherwise a no-op.
     */
    void markHostNotPrimary(const HostAndPort& host, const Status& status) override;

    /**
     * Adds host to a set of hosts marked down, otherwise a no-op.
     */
    void markHostUnreachable(const HostAndPort& host, const Status& status) override;

    /**
     * Adds host to a set of hosts marked down, otherwise a no-op.
     */
    void markHostShuttingDown(const HostAndPort& host, const Status& status) override;

    /**
     * Sets the return value for the next call to connectionString.
     */
    void setConnectionStringReturnValue(const ConnectionString returnValue);

    /**
     * Sets the return value for the next call to findHost.
     */
    void setFindHostReturnValue(StatusWith<HostAndPort> returnValue);

    void setFindHostsReturnValue(StatusWith<std::vector<HostAndPort>> returnValue);

    /**
     * Returns the current set of hosts marked down and resets the mock's internal list of marked
     * down hosts.
     */
    std::set<HostAndPort> getAndClearMarkedDownHosts();

private:
    ConnectionString _connectionStringReturnValue;
    StatusWith<std::vector<HostAndPort>> _findHostReturnValue;

    // Protects _hostsMarkedDown.
    mutable Mutex _mutex = MONGO_MAKE_LATCH("RemoteCommandTargeterMock::_mutex");

    // HostAndPorts marked not primary or unreachable. Meant to verify a code path updates the
    // RemoteCommandTargeterMock.
    std::set<HostAndPort> _hostsMarkedDown;
};

}  // namespace mongo
