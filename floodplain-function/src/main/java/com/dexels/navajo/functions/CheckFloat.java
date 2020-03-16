package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2007</p>
 * <p>Company: Dexels BV</p>
 * @author Martin Bergman
 * @version $Id$
 *
 * DISCLAIMER
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL DEXELS BV OR ITS CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 */

public class CheckFloat extends FunctionInterface {

	@Override
	public String remarks() {
		return "CheckFloat checks whether argument is valid double";
	}

	@Override
	public Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {

		Object o = operand(0).value;

		// If strict flag is set, properties can be passed as string values.
		if (getOperands().size() > 1) {

			throw new TMLExpressionException(this,"Can not use multiple args in CheckFloat");
//			String propertyName = (String) o;
//			final Message curMsg = getCurrentMessage();
//
//			Property p = (curMsg != null ? curMsg.getProperty(propertyName) : this.getNavajo().getProperty(propertyName));
//			if (p != null) {
//				o = p.getValue();
//			}
		}

		try {
			Double.parseDouble(o + "");

			return Boolean.TRUE;
		} catch (Exception e) {
			return Boolean.FALSE;
		}
	}

	@Override
	public String usage() {
		return "CheckFloat( <argument> )";
	}
}
