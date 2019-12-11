var lastUpdate = new Date();

function formatDate(d) {
    return ("0" + d.getDate()).slice(-2) + "-" + ("0" + (d.getMonth() + 1)).slice(-2) + " " + ("0" + d.getHours()).slice(-2) + ":"
            + ("0" + d.getMinutes()).slice(-2)
}

function formatDateNoTime(d) {
    return ("0" + d.getDate()).slice(-2) + "-" + ("0" + (d.getMonth() + 1)).slice(-2);
}

function formatDateTimeOnly(d) {
    return ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2)
}

$(document).ready(function() {
    Handlebars.registerHelper('dayshistory', function(n, block) {
        var dateObj = new Date();
        dateObj.setDate(dateObj.getDate() - n);
        return formatDateNoTime(dateObj);
    });

    Handlebars.registerHelper('getHistoricFailures', function(history, n, block) {
        var response = '<div title="' + translateStatus('OK') + '" class="noproblem"></div>';
        if (typeof history !== 'undefined') {
            var alerts = [];
            var checkDate = new Date();
            checkDate.setDate(checkDate.getDate() - n);
            checkDate.setHours(0, 0, 0, 0);

            $.each(history, function(idx, status) {
                var startDate = new Date(status.alertFrom);
                startDate.setHours(0, 0, 0, 0);
                if (checkDate.getTime() == startDate.getTime()) {
                    var alertString = formatDateTimeOnly(new Date(status.alertFrom)) + " ~ " + formatDateTimeOnly(new Date(status.alertTo))
                    alertString += " " + translateStatus(alert.status)
                    alerts.push(alertString);
                }
            });
            if (alerts.length > 0) {
                response = '<div class="problem" title="' + alerts.join('\n') + '"></div>';
            }
        }
        return response
    });
    
    Handlebars.registerHelper('getUptime', function(uptime, block) {
        var response = '<div class="">' + uptime + '</div>';
        return response
    });

    Handlebars.registerHelper('formatFailure', function(statusstring, cluster, options) {
        var statusclass = 'status' + statusstring;
        var tooltip = translateStatus(statusstring);
        return '<div class="' + statusclass + '" title="' + tooltip + '" > '+cluster+'</div>';
    });

    updateStatus();

});

function translateStatus(statusstring) {
    var result = 'De applicatie functioneert mogelijk niet optimaal';
    if (statusstring === "OK") {
        result = 'De applicatie functioneert normaal'
    }
    if (statusstring === 'servicedelay') {
        result = 'De applicatie functioneert niet optimaal'
    }
    if (statusstring === 'serviceunavailable') {
        result = 'De applicatie is op dit moment niet beschikbaar'
    }
    return result;
}

function updateStatus() {
    var source = $("#content-template").html();
    var template = Handlebars.compile(source);
    var statusdata = {};

    $.getJSON("/monitorapi?query=statusoverview", function(currentdata) {
        $.each(currentdata.cluster, function(cluster, status) {
            statusdata[cluster] = {
                cluster : cluster,
                current : status
            }
        });
        $.getJSON("/monitorapi?query=uptime", function(uptimedata) {
        	console.log(uptimedata);
        	 $.each(uptimedata, function(cluster, uptimedata) {
                 statusdata[cluster]['uptime'] = uptimedata;
             });
        	 
        	 $.getJSON("/monitorapi?query=historicstatusoverview", function(historicdata) {
                 $.each(historicdata, function(cluster, statuschanges) {
                     $.each(statuschanges, function(index, status) {
                         var clusterobj = statusdata[cluster];
                         if (typeof clusterobj['history'] === 'undefined') {
                             clusterobj['history'] = [ status ];
                         } else {
                             var arr = clusterobj['history'];
                             arr.push(status);
                             clusterobj['history'] = arr;
                         }
                     });

                 });
                 lastUpdate = new Date();
                 var statusobj = {
                     status : []
                 };
                 $.each(statusdata, function(cluster, obj) {
                     statusobj.status.push(obj)
                 });
                 $("#status").html(template(statusobj));
             });
        });
       
    });
};

