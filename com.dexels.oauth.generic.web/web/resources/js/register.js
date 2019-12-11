$.getJSON("/oauth/loginstatus", function(data) {
	$(function() {
		handleLoginStatus(data);
	});
});

$(function() {
	
	$(document).on('click', '#to-login-btn', function(event) {
		event.preventDefault();
		var type = window.location.hash.substr(1);
		window.location.href = 'login.html#' + type;
		return true;
	});
   
	 $('#register-form' ).submit( function( event ) {
		hideAlert();
        event.preventDefault();
        var username = $( '#register-username' ).val();
        var password = $( '#register-password' ).val();
        var passwordRepeat = $( '#register-password-repeat' ).val();
        
        if(!username || !password || !passwordRepeat ) {
            displayAlert( 'danger', '', "Vul alle velden in" );
            return;
        }
        
        if (!email_regex.test(username)) {
            displayAlert( 'danger', '', "Ongeldig e-mailadres ingevoerd" );
            return;
        }
        
        if( password.length < 8 ) {
            displayAlert( 'danger', '', "Een wachtwoord dient minimaal 8 tekens lang te zijn" );
            return;
        }
        
        if( password != passwordRepeat ) {
            displayAlert( 'danger', '', "Wachtwoorden zijn niet gelijk" );
            return;
        }
        var parameters = {
            username: encodeURIComponent(username),
            password: encodeURIComponent(password),
        };
        
        var components = [];
        $.each(parameters, function(key, value) {
            components.push(key + "=" + value);
        });
        
        $.getJSON("/oauth/register?" + components.join('&'), function(data) {
            if(data.isSuccessful) {
            	
            	$('#registerform').html('')
            	
            	var type = window.location.hash.substr(1);
            	
        		var resultheader = $('<div>').attr('class', 'mb-4 pb-3 h5 text-center')
            	resultheader.text('Bedankt voor je registratie!');
            	
            	var resulttext = $('<p>');
            	resulttext.text(' Je hoeft je account nu alleen nog te bevestigen. Klik op de activatie-link in de e-mail die we zojuist hebben toegezonden om direct aan de slag te gaan. (niet actief in demo versie)')
            	
            	var type = window.location.hash.substr(1);
            	var link = $('<a>').attr("href", "login.html#" + type );
            	link.text('hier');
            	var forwardtext = $('<p>');
            	forwardtext.text('Klik ')
            	forwardtext.append(link);
            	forwardtext.append(' om in te loggen nadat je je account geactiveerd hebt.');
            	
            	
            	$('#registerform').append(resultheader);
            	$('#registerform').append(resulttext);
            	$('#registerform').append(forwardtext);
            	

            } else {
                displayAlert('danger', data.title, data.message);
            }
           
        });
        

	} );
    
});
